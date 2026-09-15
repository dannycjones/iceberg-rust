// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Column projection for `ArrowReader`: building the Parquet projection mask
//! from Iceberg field IDs, and mapping field IDs between Iceberg and Parquet
//! (including fallback handling for files without embedded IDs).

use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;

use arrow_schema::{Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use parquet::arrow::{PARQUET_FIELD_ID_META_KEY, ProjectionMask};
use parquet::schema::types::{SchemaDescriptor, Type as ParquetType};

use super::{ArrowReader, CollectFieldIdVisitor};
use crate::arrow::arrow_schema_to_schema;
use crate::error::Result;
use crate::expr::BoundPredicate;
use crate::expr::visitors::bound_predicate_visitor::visit;
use crate::spec::{NameMapping, NestedField, PrimitiveType, Schema, Type};
use crate::{Error, ErrorKind};

impl ArrowReader {
    /// The Iceberg field ids a scan's filter predicate references.
    pub(super) fn collect_predicate_field_ids(predicate: &BoundPredicate) -> Result<Vec<i32>> {
        let mut collector = CollectFieldIdVisitor {
            field_ids: HashSet::default(),
        };
        visit(&mut collector, predicate)?;
        Ok(collector.field_ids.into_iter().collect())
    }

    pub(super) fn build_field_id_set_and_map(
        parquet_schema: &SchemaDescriptor,
        arrow_schema: &ArrowSchemaRef,
        predicate: &BoundPredicate,
        use_position_fallback: bool,
    ) -> Result<(HashSet<i32>, HashMap<i32, usize>)> {
        // Collects all Iceberg field IDs referenced in the filter predicate
        let mut collector = CollectFieldIdVisitor {
            field_ids: HashSet::default(),
        };
        visit(&mut collector, predicate)?;

        let iceberg_field_ids = collector.field_ids();

        let field_id_map = match build_field_id_map(parquet_schema)? {
            Some(map) => map,
            // No embedded field IDs and no name mapping: position-based fallback
            None if use_position_fallback => build_fallback_field_id_map(parquet_schema),
            // No embedded field IDs, but a name mapping assigned them to the Arrow
            // schema: resolve columns through the mapped Arrow field-id metadata
            None => build_field_id_map_from_arrow_schema(arrow_schema),
        };

        Ok((iceberg_field_ids, field_id_map))
    }

    /// Recursively extract leaf field IDs because Parquet projection works at the leaf column level.
    /// Nested types (struct/list/map) are flattened in Parquet's columnar format.
    fn include_leaf_field_id(field: &NestedField, field_ids: &mut Vec<i32>) {
        match field.field_type.as_ref() {
            Type::Primitive(_) => {
                field_ids.push(field.id);
            }
            Type::Struct(struct_type) => {
                for nested_field in struct_type.fields() {
                    Self::include_leaf_field_id(nested_field, field_ids);
                }
            }
            Type::List(list_type) => {
                Self::include_leaf_field_id(&list_type.element_field, field_ids);
            }
            Type::Map(map_type) => {
                Self::include_leaf_field_id(&map_type.key_field, field_ids);
                Self::include_leaf_field_id(&map_type.value_field, field_ids);
            }
            // Variant projection is rejected earlier (in `get_arrow_projection_mask`); this
            // arm only keeps the match exhaustive. Treat it as a leaf, like a primitive.
            Type::Variant(_) => {
                field_ids.push(field.id);
            }
        }
    }

    /// `predicate_field_ids` are the ids the scan's filter references. They are type-checked but
    /// not projected: a filter column is read from the file to evaluate the predicate, so an
    /// unreadable type there is just as wrong as in the projection, but it must not be added to
    /// the returned mask or it would appear in the output batch.
    ///
    /// They are a separate argument because they are not a subset of `field_ids` --
    /// `collect_scan_field_ids` (`crate::scan`) derives `project_field_ids` from the selected
    /// column names alone, so `select(["a"]).filter(b > ...)` never mentions `b` here otherwise.
    pub(super) fn get_arrow_projection_mask(
        field_ids: &[i32],
        predicate_field_ids: &[i32],
        iceberg_schema_of_task: &Schema,
        parquet_schema: &SchemaDescriptor,
        arrow_schema: &ArrowSchemaRef,
        use_fallback: bool, // Position-based fallback: file lacks embedded field IDs and no name mapping assigned any
    ) -> Result<ProjectionMask> {
        if field_ids.is_empty() && predicate_field_ids.is_empty() {
            return Ok(ProjectionMask::all());
        }

        // Reading variant columns is not supported yet (see #2188 follow-ups): reject any
        // projection that touches a variant, rather than returning a partial/incorrect batch.
        for field_id in field_ids {
            if let Some(field) = iceberg_schema_of_task.field_by_id(*field_id)
                && type_contains_variant(&field.field_type)
            {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    "Reading variant columns is not supported yet",
                ));
            }
        }

        if use_fallback {
            // Position-based projection necessary because file lacks embedded field IDs.
            // Nothing is type-checked on this path, so `predicate_field_ids` has nothing to
            // contribute -- see the FOLLOW-UP note on the function.
            if field_ids.is_empty() {
                return Ok(ProjectionMask::all());
            }
            Self::get_arrow_projection_mask_fallback(field_ids, parquet_schema)
        } else {
            // Field-ID-based projection using embedded field IDs from Parquet metadata

            // Parquet's columnar format requires leaf-level (not top-level struct/list/map) projection
            let leaves_of = |ids: &[i32]| {
                let mut leaves = vec![];
                for field_id in ids {
                    if let Some(field) = iceberg_schema_of_task.field_by_id(*field_id) {
                        Self::include_leaf_field_id(field, &mut leaves);
                    }
                }
                leaves
            };

            Self::get_arrow_projection_mask_with_field_ids(
                &leaves_of(field_ids),
                &leaves_of(predicate_field_ids),
                iceberg_schema_of_task,
                parquet_schema,
                arrow_schema,
            )
        }
    }

    /// Standard projection using embedded field IDs from Parquet metadata.
    /// For iceberg-java compatibility with ParquetSchemaUtil.pruneColumns().
    fn get_arrow_projection_mask_with_field_ids(
        leaf_field_ids: &[i32],
        predicate_leaf_field_ids: &[i32],
        iceberg_schema_of_task: &Schema,
        parquet_schema: &SchemaDescriptor,
        arrow_schema: &ArrowSchemaRef,
    ) -> Result<ProjectionMask> {
        let mut column_map = HashMap::new();
        let fields = arrow_schema.fields();
        // HashSet for O(1) membership checks instead of O(n) slice scans.
        //
        // `checked_field_id_set` is the union of the projected and filtered leaves: every leaf
        // whose type must be validated. It drives the pre-projection below (a leaf's type is
        // only available once it has been converted) and the type-check pass. The returned mask
        // is still built from `leaf_field_ids` alone, so filter-only columns are validated
        // without being read into the output batch.
        let leaf_field_id_set: HashSet<i32> = leaf_field_ids.iter().copied().collect();
        let checked_field_id_set: HashSet<i32> = leaf_field_id_set
            .iter()
            .chain(predicate_leaf_field_ids)
            .copied()
            .collect();

        // Pre-project only the fields that have been selected, possibly avoiding converting
        // some Arrow types that are not yet supported.
        //
        // The ids seen here are recorded by leaf index for the second pass to reuse, rather
        // than in a map keyed by the `FieldRef`: Arrow hashes a `Field` by
        // name/type/nullability/metadata and `Arc<Field>` delegates to that, so two
        // structurally identical leaves -- which Parquet and Arrow both permit, and which
        // `apply_name_mapping_to_arrow_schema` can even assign the same id -- would collapse
        // into one entry, and the second pass would then type-check one leaf while projecting
        // another. Both passes visit leaves in the same order, so the index is a stable,
        // collision-free key.
        let mut projected_leaf_ids: Vec<Option<i32>> = vec![];
        let projected_arrow_schema = ArrowSchema::new_with_metadata(
            fields.filter_leaves(|idx, f| {
                let field_id = f
                    .metadata()
                    .get(PARQUET_FIELD_ID_META_KEY)
                    .and_then(|field_id| i32::from_str(field_id).ok());

                debug_assert_eq!(idx, projected_leaf_ids.len(), "leaf visit order changed");
                projected_leaf_ids.resize(idx + 1, None);
                projected_leaf_ids[idx] = field_id;

                field_id.is_some_and(|field_id| checked_field_id_set.contains(&field_id))
            }),
            arrow_schema.metadata().clone(),
        );
        let iceberg_schema = arrow_schema_to_schema(&projected_arrow_schema)?;

        // Collect the columns whose file type cannot be promoted to the projected type and fail
        // once the closure has returned, rather than skipping them: skipping makes such a
        // column indistinguishable from one that is genuinely absent from the file, and the
        // NULL-filling below (and in `RecordBatchTransformer`) would then silently mask
        // real values that are on disk but unreadable at the projected type.
        //
        // Accumulated rather than returned from the closure because `filter_leaves` yields
        // `bool`. Arrow does expose `try_filter_leaves`, so reporting from inside the closure
        // is possible; it would mean threading `Result` through both passes for no gain over
        // collecting, and collecting has the advantage of naming every bad column at once.
        let mut type_mismatches: Vec<String> = vec![];

        fields.filter_leaves(|idx, _field| {
            let Some(field_id) = projected_leaf_ids.get(idx).copied().flatten() else {
                return false;
            };

            // This closure visits every leaf carrying a parseable field id, not just the
            // requested ones: `projected_leaf_ids` above records the id before the
            // `checked_field_id_set` test. `iceberg_schema` is built from the pre-projected
            // Arrow schema, so a `None` here means this leaf is neither projected nor filtered
            // on -- it is not a judgement about the file. Skipping is right either way, and it
            // keeps the type check scoped to leaves this scan actually reads.
            let (Some(iceberg_field), Some(parquet_iceberg_field)) = (
                iceberg_schema_of_task.field_by_id(field_id),
                iceberg_schema.field_by_id(field_id),
            ) else {
                return false;
            };

            if !type_promotion_is_valid(
                parquet_iceberg_field.field_type.as_primitive_type(),
                iceberg_field.field_type.as_primitive_type(),
            ) {
                let mismatch = format!(
                    "field {} ({}): file type {}, projected type {}",
                    field_id,
                    iceberg_field.name,
                    parquet_iceberg_field.field_type,
                    iceberg_field.field_type
                );
                // A field id can be requested more than once, and two distinct leaves can
                // carry the same embedded id, so guard against naming one column twice as
                // though it were two separate problems.
                if !type_mismatches.contains(&mismatch) {
                    type_mismatches.push(mismatch);
                }
                return false;
            }

            column_map.insert(field_id, idx);

            // `false`, not `true`: the rebuilt `Fields` tree this would produce is discarded.
            // Returning `false` throughout skips cloning every struct/list/map wrapper and does
            // not perturb this pass -- arrow increments its leaf counter unconditionally and
            // recurses into all children regardless of the predicate -- so leaf indices and
            // visit order are unaffected. Same idiom as `build_field_id_map_from_arrow_schema`.
            false
        });

        // We must first check for any unpromotable column types.
        // Once we've rejected them, we can trust that a field ID absent in `column_map` now means the data file
        // was written without the column and default values should be used later.
        if !type_mismatches.is_empty() {
            return Err(unpromotable_column_types_error(&type_mismatches));
        }

        // `leaf_field_ids`, not `checked_field_id_set`: `column_map` also holds the filter-only
        // leaves that were type-checked above, and those must not be projected into the output
        // batch. They are read by the row filter's own mask (`get_row_filter`) instead.
        let mut indices = vec![];
        for field_id in leaf_field_ids {
            if let Some(col_idx) = column_map.get(field_id) {
                indices.push(*col_idx);
            }
        }

        if indices.is_empty() {
            // Edge case: All requested columns are new (don't exist in file).
            // Project all columns so RecordBatchTransformer has a batch to transform.
            Ok(ProjectionMask::all())
        } else {
            Ok(ProjectionMask::leaves(parquet_schema, indices))
        }
    }

    /// Fallback projection for Parquet files without field IDs.
    /// Uses position-based matching: field ID N → column position N-1.
    /// Projects entire top-level columns (including nested content) for iceberg-java compatibility.
    ///
    /// The positional guess is a heuristic (matching iceberg-java's
    /// `ParquetSchemaUtil.addFallbackIds()`), so a file whose physical column order does not line
    /// up with the schema's field ids is misread here. Nothing detects that: a type check would
    /// catch it only when the two columns happen to differ in type.
    fn get_arrow_projection_mask_fallback(
        field_ids: &[i32],
        parquet_schema: &SchemaDescriptor,
    ) -> Result<ProjectionMask> {
        // Position-based: field_id N → column N-1 (field IDs are 1-indexed)
        let parquet_root_fields = parquet_schema.root_schema().get_fields();
        let mut root_indices = vec![];

        // FOLLOW-UP: this path does not type-check at all, so the silent-NULL problem fixed
        // above still exists here in a narrower form -- see
        // `test_fallback_projection_silently_nulls_unrepresentable_values`. Applying
        // `type_promotion_is_valid` here is NOT the fix: that allowlist answers "is this a
        // legal Iceberg promotion", but files reaching this path were never written by an
        // Iceberg writer, so their physical types diverge by construction (Hive `string` as
        // unannotated `binary`, naive `timestamp` under a `timestamptz` schema). Pre-screening
        // on it rejects those files even though the cast reads them correctly. A strict
        // (`safe: false`) cast in `RecordBatchTransformer` is the promising direction: it
        // permits every lossless cast and fails only on the values that would become NULL.
        for field_id in field_ids.iter() {
            let parquet_pos = (*field_id - 1) as usize;

            if parquet_pos < parquet_root_fields.len() {
                root_indices.push(parquet_pos);
            }
            // RecordBatchTransformer adds missing columns with NULL values
        }

        if root_indices.is_empty() {
            Ok(ProjectionMask::all())
        } else {
            Ok(ProjectionMask::roots(parquet_schema, root_indices))
        }
    }
}

/// Whether a Parquet file column of `file_type` can be read as `projected_type`.
///
/// Deliberately narrower than the spec's promotion table, and narrower still than what
/// `arrow_cast::cast` can do. Only meaningful for files carrying embedded field ids, whose
/// writer was Iceberg-aware and so should only ever have produced a legal promotion.
fn type_promotion_is_valid(
    file_type: Option<&PrimitiveType>,
    projected_type: Option<&PrimitiveType>,
) -> bool {
    match (file_type, projected_type) {
        (Some(lhs), Some(rhs)) if lhs == rhs => true,
        (Some(PrimitiveType::Int), Some(PrimitiveType::Long)) => true,
        (Some(PrimitiveType::Float), Some(PrimitiveType::Double)) => true,
        (
            Some(PrimitiveType::Decimal {
                precision: file_precision,
                scale: file_scale,
            }),
            Some(PrimitiveType::Decimal {
                precision: requested_precision,
                scale: requested_scale,
            }),
        ) if requested_precision >= file_precision && file_scale == requested_scale => true,
        // Uuid will be store as Fixed(16) in parquet file, so the read back type will be Fixed(16).
        (Some(PrimitiveType::Fixed(16)), Some(PrimitiveType::Uuid)) => true,
        _ => false,
    }
}

/// The error for columns present in the file but unreadable at the projected type.
///
/// `FeatureUnsupported`, not `DataInvalid`: this fires whenever a pair is missing from
/// `type_promotion_is_valid`'s allowlist, and that allowlist is narrower than the spec. Some
/// pairs really are invalid data (the spec forbids narrowing, so a `long` column under an `int`
/// schema cannot come from a valid schema history), but others are perfectly valid files this
/// reader cannot yet interpret -- a Parquet `Geometry` column, which arrow-rs surfaces as
/// `Binary`, or the v3 `date` -> `timestamp` promotion the allowlist omits. One kind covers
/// both, so prefer the one that does not tell users their valid files are corrupt.
fn unpromotable_column_types_error(type_mismatches: &[String]) -> Error {
    Error::new(
        ErrorKind::FeatureUnsupported,
        format!(
            "Parquet file column types cannot be promoted to the projected schema types: {}",
            type_mismatches.join("; ")
        ),
    )
}

/// Whether `field_type` is, or transitively contains, a variant type.
fn type_contains_variant(field_type: &Type) -> bool {
    match field_type {
        Type::Variant(_) => true,
        Type::Struct(s) => s
            .fields()
            .iter()
            .any(|f| type_contains_variant(&f.field_type)),
        Type::List(l) => type_contains_variant(&l.element_field.field_type),
        Type::Map(m) => {
            type_contains_variant(&m.key_field.field_type)
                || type_contains_variant(&m.value_field.field_type)
        }
        Type::Primitive(_) => false,
    }
}

/// Build the map of parquet field id to Parquet column index in the schema.
/// Returns None if the Parquet file doesn't have field IDs embedded (e.g., migrated tables).
pub(super) fn build_field_id_map(
    parquet_schema: &SchemaDescriptor,
) -> Result<Option<HashMap<i32, usize>>> {
    let mut column_map = HashMap::new();

    for (idx, field) in parquet_schema.columns().iter().enumerate() {
        let field_type = field.self_type();
        match field_type {
            ParquetType::PrimitiveType { basic_info, .. } => {
                if !basic_info.has_id() {
                    return Ok(None);
                }
                column_map.insert(basic_info.id(), idx);
            }
            ParquetType::GroupType { .. } => {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Leaf column in schema should be primitive type but got {field_type:?}"
                    ),
                ));
            }
        };
    }

    Ok(Some(column_map))
}

/// Finds the Parquet leaf column index carrying `field_id` by its embedded id.
///
/// Unlike [`build_field_id_map`], a leaf without an embedded id does not abort the
/// search -- it is simply skipped. This tolerates files that legitimately mix id-bearing
/// and id-less leaves, e.g. a Variant column whose internal metadata/value leaves are
/// required by the spec to have no field id, alongside a reserved metadata column that
/// does carry its id.
pub(super) fn find_leaf_by_field_id(
    parquet_schema: &SchemaDescriptor,
    field_id: i32,
) -> Option<usize> {
    parquet_schema.columns().iter().position(|col| {
        matches!(
            col.self_type(),
            ParquetType::PrimitiveType { basic_info, .. }
                if basic_info.has_id() && basic_info.id() == field_id
        )
    })
}

/// Build a fallback field ID map for Parquet files without embedded field IDs.
///
/// Returns the number of primitive (leaf) columns in a Parquet type, recursing into groups.
fn leaf_count(ty: &parquet::schema::types::Type) -> usize {
    if ty.is_primitive() {
        1
    } else {
        ty.get_fields().iter().map(|f| leaf_count(f)).sum()
    }
}

/// Builds a mapping from fallback field IDs to leaf column indices for Parquet files
/// without embedded field IDs. Returns entries only for primitive top-level fields.
///
/// Must use top-level field positions (not leaf column positions) to stay consistent
/// with `add_fallback_field_ids_to_arrow_schema`, which assigns ordinal IDs to
/// top-level Arrow fields. Using leaf positions instead would produce wrong indices
/// when nested types (struct/list/map) expand into multiple leaf columns.
///
/// Mirrors iceberg-java's ParquetSchemaUtil.addFallbackIds() which iterates
/// fileSchema.getFields() assigning ordinal IDs to top-level fields.
pub(super) fn build_fallback_field_id_map(
    parquet_schema: &SchemaDescriptor,
) -> HashMap<i32, usize> {
    let mut column_map = HashMap::new();
    let mut leaf_idx = 0;

    for (top_pos, field) in parquet_schema.root_schema().get_fields().iter().enumerate() {
        let field_id = (top_pos + 1) as i32;
        if field.is_primitive() {
            column_map.insert(field_id, leaf_idx);
        }
        leaf_idx += leaf_count(field);
    }

    column_map
}

/// Builds a mapping from field IDs to leaf column indices using the field-id metadata
/// carried by the Arrow schema.
///
/// Used for Parquet files without embedded field IDs when a name mapping has assigned
/// IDs to the Arrow schema (see [`apply_name_mapping_to_arrow_schema`]): the Parquet
/// schema descriptor itself still has no IDs, but the Arrow leaves are flattened in the
/// same depth-first order as Parquet leaf columns, so the Arrow leaf index lines up with
/// the Parquet column index. Columns the mapping did not match carry no field-id
/// metadata and are simply absent from the map.
fn build_field_id_map_from_arrow_schema(arrow_schema: &ArrowSchemaRef) -> HashMap<i32, usize> {
    let mut column_map = HashMap::new();
    arrow_schema.fields().filter_leaves(|idx, field| {
        if let Some(field_id) = field
            .metadata()
            .get(PARQUET_FIELD_ID_META_KEY)
            .and_then(|value| i32::from_str(value).ok())
        {
            column_map.insert(field_id, idx);
        }
        false
    });
    column_map
}

/// Apply name mapping to Arrow schema for Parquet files lacking field IDs.
///
/// Assigns Iceberg field IDs based on column names using the name mapping,
/// enabling correct projection on migrated files (e.g., from Hive/Spark via add_files).
///
/// Per Iceberg spec Column Projection rule #2:
/// "Use schema.name-mapping.default metadata to map field id to columns without field id"
/// https://iceberg.apache.org/spec/#column-projection
///
/// Corresponds to Java's ParquetSchemaUtil.applyNameMapping() and ApplyNameMapping visitor.
/// The key difference is Java operates on Parquet MessageType, while we operate on Arrow Schema.
///
/// # Arguments
/// * `arrow_schema` - Arrow schema from Parquet file (without field IDs)
/// * `name_mapping` - Name mapping from table metadata (TableProperties.DEFAULT_NAME_MAPPING)
///
/// # Returns
/// Arrow schema with field IDs assigned based on name mapping
pub(super) fn apply_name_mapping_to_arrow_schema(
    arrow_schema: ArrowSchemaRef,
    name_mapping: &NameMapping,
) -> Result<Arc<ArrowSchema>> {
    debug_assert!(
        arrow_schema
            .fields()
            .iter()
            .next()
            .is_none_or(|f| f.metadata().get(PARQUET_FIELD_ID_META_KEY).is_none()),
        "Schema already has field IDs - name mapping should not be applied"
    );

    let fields_with_mapped_ids: Vec<_> = arrow_schema
        .fields()
        .iter()
        .map(|field| {
            // Look up this column name in name mapping to get the Iceberg field ID.
            // Corresponds to Java's ApplyNameMapping visitor which calls
            // nameMapping.find(currentPath()) and returns field.withId() if found.
            //
            // If the field isn't in the mapping, leave it WITHOUT assigning an ID
            // (matching Java's behavior of returning the field unchanged).
            // Later, during projection, fields without IDs are filtered out.
            let mapped_field_opt = name_mapping
                .fields()
                .iter()
                .find(|f| f.names().contains(&field.name().to_string()));

            let mut metadata = field.metadata().clone();

            if let Some(mapped_field) = mapped_field_opt
                && let Some(field_id) = mapped_field.field_id()
            {
                // Field found in mapping with a field_id → assign it
                metadata.insert(PARQUET_FIELD_ID_META_KEY.to_string(), field_id.to_string());
            }
            // If field_id is None, leave the field without an ID (will be filtered by projection)

            Field::new(field.name(), field.data_type().clone(), field.is_nullable())
                .with_metadata(metadata)
        })
        .collect();

    Ok(Arc::new(ArrowSchema::new_with_metadata(
        fields_with_mapped_ids,
        arrow_schema.metadata().clone(),
    )))
}

/// Add position-based fallback field IDs to Arrow schema for Parquet files lacking them.
/// Enables projection on migrated files (e.g., from Hive/Spark).
///
/// Why at schema level (not per-batch): Efficiency - avoids repeated schema modification.
/// Why only top-level: Nested projection uses leaf column indices, not parent struct IDs.
/// Why 1-indexed: Compatibility with iceberg-java's ParquetSchemaUtil.addFallbackIds().
pub(super) fn add_fallback_field_ids_to_arrow_schema(
    arrow_schema: &ArrowSchemaRef,
) -> Arc<ArrowSchema> {
    debug_assert!(
        arrow_schema
            .fields()
            .iter()
            .next()
            .is_none_or(|f| f.metadata().get(PARQUET_FIELD_ID_META_KEY).is_none()),
        "Schema already has field IDs"
    );

    let fields_with_fallback_ids: Vec<_> = arrow_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(pos, field)| {
            let mut metadata = field.metadata().clone();
            let field_id = (pos + 1) as i32; // 1-indexed for Java compatibility
            metadata.insert(PARQUET_FIELD_ID_META_KEY.to_string(), field_id.to_string());

            Field::new(field.name(), field.data_type().clone(), field.is_nullable())
                .with_metadata(metadata)
        })
        .collect();

    Arc::new(ArrowSchema::new_with_metadata(
        fields_with_fallback_ids,
        arrow_schema.metadata().clone(),
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::File;
    use std::sync::Arc;

    use arrow_array::cast::AsArray;
    use arrow_array::{Array, ArrayRef, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema, TimeUnit};
    use futures::TryStreamExt;
    use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY, ProjectionMask};
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;
    use parquet::schema::parser::parse_message_type;
    use parquet::schema::types::SchemaDescriptor;
    use tempfile::TempDir;

    use crate::arrow::{ArrowReader, ArrowReaderBuilder};
    use crate::expr::{Bind, Reference};
    use crate::io::FileIO;
    use crate::scan::{FileScanTask, FileScanTaskStream};
    use crate::spec::{
        DataFileFormat, Datum, MappedField, NameMapping, NestedField, PrimitiveType, Schema,
        StructType, Type, VariantType,
    };
    use crate::{ErrorKind, Runtime};

    #[test]
    fn test_arrow_projection_mask() {
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_identifier_field_ids(vec![1])
                .with_fields(vec![
                    NestedField::required(1, "c1", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::optional(2, "c2", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(
                        3,
                        "c3",
                        Type::Primitive(PrimitiveType::Decimal {
                            precision: 38,
                            scale: 3,
                        }),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("c1", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
            // Type not supported
            Field::new("c2", DataType::Duration(TimeUnit::Microsecond), true).with_metadata(
                HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "2".to_string())]),
            ),
            // Precision is beyond the supported range
            Field::new("c3", DataType::Decimal128(39, 3), true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "3".to_string(),
            )])),
        ]));

        let message_type = "
message schema {
  required binary c1 (STRING) = 1;
  optional int32 c2 (INTEGER(8,true)) = 2;
  optional fixed_len_byte_array(17) c3 (DECIMAL(39,3)) = 3;
}
    ";
        let parquet_type = parse_message_type(message_type).expect("should parse schema");
        let parquet_schema = SchemaDescriptor::new(Arc::new(parquet_type));

        // Try projecting the fields c2 and c3 with the unsupported data types
        let err = ArrowReader::get_arrow_projection_mask(
            &[1, 2, 3],
            &[],
            &schema,
            &parquet_schema,
            &arrow_schema,
            false,
        )
        .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert_eq!(
            err.to_string(),
            "DataInvalid => Unsupported Arrow data type: Duration(µs)".to_string()
        );

        // Omitting field c2, we still get an error due to c3 being selected
        let err = ArrowReader::get_arrow_projection_mask(
            &[1, 3],
            &[],
            &schema,
            &parquet_schema,
            &arrow_schema,
            false,
        )
        .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert_eq!(
            err.to_string(),
            "DataInvalid => Failed to create decimal type, source: DataInvalid => Decimals with precision larger than 38 are not supported: 39".to_string()
        );

        // Finally avoid selecting fields with unsupported data types
        let mask = ArrowReader::get_arrow_projection_mask(
            &[1],
            &[],
            &schema,
            &parquet_schema,
            &arrow_schema,
            false,
        )
        .expect("Some ProjectionMask");
        assert_eq!(mask, ProjectionMask::leaves(&parquet_schema, vec![0]));
    }

    /// A column that is present in the file but whose type cannot be promoted to the
    /// projected type must error, not be skipped. Skipping it would make it
    /// indistinguishable from a column that is absent from the file, and the reader would
    /// NULL-fill it -- returning NULLs for values that are on disk, with no error or
    /// warning. Spec column-projection rule #4 (null for a missing column) is scoped to
    /// fields *not present* in a data file; a present-but-unreadable column is not in scope.
    ///
    /// Distinct from `test_arrow_projection_mask`, which pins the *unsupported Arrow type*
    /// error raised earlier by `arrow_schema_to_schema`. The gap here is narrower: an Arrow
    /// type that converts to an Iceberg type perfectly well, but whose promotion to the
    /// projected type `type_promotion_is_valid` does not permit.
    #[test]
    fn test_arrow_projection_mask_rejects_invalid_type_promotion() {
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "c1", Type::Primitive(PrimitiveType::String)).into(),
                    // Narrowing Long -> Int is not a valid promotion.
                    NestedField::optional(2, "c2", Type::Primitive(PrimitiveType::Int)).into(),
                    // Binary -> Uuid is not a valid promotion (only Fixed(16) -> Uuid is).
                    NestedField::optional(3, "c3", Type::Primitive(PrimitiveType::Uuid)).into(),
                ])
                .build()
                .unwrap(),
        );

        let field_with_id = |name: &str, data_type: DataType, id: &str| {
            Field::new(name, data_type, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                id.to_string(),
            )]))
        };

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            field_with_id("c1", DataType::Utf8, "1"),
            field_with_id("c2", DataType::Int64, "2"),
            field_with_id("c3", DataType::Binary, "3"),
        ]));

        let message_type = "
message schema {
  required binary c1 (STRING) = 1;
  optional int64 c2 = 2;
  optional binary c3 = 3;
}
    ";
        let parquet_type = parse_message_type(message_type).expect("should parse schema");
        let parquet_schema = SchemaDescriptor::new(Arc::new(parquet_type));

        // Every mismatching column is named, not just the first one found.
        let err = ArrowReader::get_arrow_projection_mask(
            &[1, 2, 3],
            &[],
            &schema,
            &parquet_schema,
            &arrow_schema,
            false,
        )
        .expect_err("unpromotable column types must be rejected");

        assert_eq!(err.kind(), ErrorKind::FeatureUnsupported);
        let message = err.to_string();
        assert!(
            message.contains("field 2 (c2): file type long, projected type int"),
            "{message}"
        );
        assert!(
            message.contains("field 3 (c3): file type binary, projected type uuid"),
            "{message}"
        );

        // The promotable column on its own still projects.
        let mask = ArrowReader::get_arrow_projection_mask(
            &[1],
            &[],
            &schema,
            &parquet_schema,
            &arrow_schema,
            false,
        )
        .expect("Some ProjectionMask");
        assert_eq!(mask, ProjectionMask::leaves(&parquet_schema, vec![0]));
    }

    /// The error above must not swallow the genuine not-present case: a field id that the
    /// file does not carry at all is still NULL-filled per column-projection rule #4.
    #[test]
    fn test_arrow_projection_mask_allows_missing_column() {
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "c1", Type::Primitive(PrimitiveType::String)).into(),
                    // Added after the file was written.
                    NestedField::optional(2, "c2", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("c1", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
        ]));

        let parquet_schema = SchemaDescriptor::new(Arc::new(
            parse_message_type("message schema { required binary c1 (STRING) = 1; }").unwrap(),
        ));

        let mask = ArrowReader::get_arrow_projection_mask(
            &[1, 2],
            &[],
            &schema,
            &parquet_schema,
            &arrow_schema,
            false,
        )
        .expect("a column absent from the file must not be an error");
        assert_eq!(mask, ProjectionMask::leaves(&parquet_schema, vec![0]));
    }

    #[test]
    fn test_arrow_projection_mask_variant_is_unsupported() {
        // Reading variant columns is not supported yet: projecting one (top-level or
        // nested) must fail loudly rather than return a partial/incorrect batch.
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(2, "v", Type::Variant(VariantType)).into(),
                    NestedField::required(
                        3,
                        "s",
                        Type::Struct(StructType::new(vec![
                            NestedField::optional(4, "vv", Type::Variant(VariantType)).into(),
                        ])),
                    )
                    .into(),
                    NestedField::required(
                        5,
                        "m",
                        Type::Map(crate::spec::MapType::required(
                            6,
                            Type::Primitive(PrimitiveType::String),
                            7,
                            Type::Variant(VariantType),
                        )),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        // The parquet/arrow schemas are irrelevant: the variant is rejected before they
        // are consulted, so an empty descriptor is enough to drive the code path.
        let parquet_schema = SchemaDescriptor::new(Arc::new(
            parse_message_type("message schema { optional int32 id = 1; }").unwrap(),
        ));
        let arrow_schema = Arc::new(ArrowSchema::empty());

        // 2 = top-level variant, 3 = struct containing a variant, 4 = the nested variant,
        // 5 = map<string, variant>, plus a mix with a non-variant sibling.
        for projected in [vec![2], vec![3], vec![4], vec![5], vec![1, 2]] {
            let err = ArrowReader::get_arrow_projection_mask(
                &projected,
                &[],
                &schema,
                &parquet_schema,
                &arrow_schema,
                false,
            )
            .expect_err("variant projection must be rejected");
            assert_eq!(err.kind(), ErrorKind::FeatureUnsupported, "{err}");
        }
    }

    /// Test schema evolution: reading old Parquet file (with only column 'a')
    /// using a newer table schema (with columns 'a' and 'b').
    /// This tests that:
    /// 1. get_arrow_projection_mask allows missing columns
    /// 2. RecordBatchTransformer adds missing column 'b' with NULL values
    #[tokio::test]
    async fn test_schema_evolution_add_column() {
        use arrow_array::{Array, Int32Array};

        // New table schema: columns 'a' and 'b' (b was added later, file only has 'a')
        let new_schema = Arc::new(
            Schema::builder()
                .with_schema_id(2)
                .with_fields(vec![
                    NestedField::required(1, "a", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(2, "b", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        // Create Arrow schema for old Parquet file (only has column 'a')
        let arrow_schema_old = Arc::new(ArrowSchema::new(vec![
            Field::new("a", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
        ]));

        // Write old Parquet file with only column 'a'
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        let data_a = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let to_write = RecordBatch::try_new(arrow_schema_old.clone(), vec![data_a]).unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let file = File::create(format!("{table_location}/old_file.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, to_write.schema(), Some(props)).unwrap();
        writer.write(&to_write).expect("Writing batch");
        writer.close().unwrap();

        // Read the old Parquet file using the NEW schema (with column 'b')
        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();
        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask::builder()
                .with_file_size_in_bytes(
                    std::fs::metadata(format!("{table_location}/old_file.parquet"))
                        .unwrap()
                        .len(),
                )
                .with_start(0)
                .with_length(0)
                .with_data_file_path(format!("{table_location}/old_file.parquet"))
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(new_schema.clone())
                .with_project_field_ids(vec![1, 2]) // Request both columns 'a' and 'b'
                .with_case_sensitive(false)
                .build()
                .unwrap())]
            .into_iter(),
        )) as FileScanTaskStream;

        let result = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        // Verify we got the correct data
        assert_eq!(result.len(), 1);
        let batch = &result[0];

        // Should have 2 columns now
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.num_rows(), 3);

        // Column 'a' should have the original data
        let col_a = batch
            .column(0)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(col_a.values(), &[1, 2, 3]);

        // Column 'b' should be all NULLs (it didn't exist in the old file)
        let col_b = batch
            .column(1)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(col_b.null_count(), 3);
        assert!(col_b.is_null(0));
        assert!(col_b.is_null(1));
        assert!(col_b.is_null(2));
    }

    /// Test reading Parquet files without field ID metadata (e.g., migrated tables).
    /// This exercises the position-based fallback path.
    ///
    /// Corresponds to Java's ParquetSchemaUtil.addFallbackIds() + pruneColumnsFallback()
    /// in /parquet/src/main/java/org/apache/iceberg/parquet/ParquetSchemaUtil.java
    #[tokio::test]
    async fn test_read_parquet_file_without_field_ids() {
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "name", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(2, "age", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        // Parquet file from a migrated table - no field ID metadata
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, false),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        let name_data = vec!["Alice", "Bob", "Charlie"];
        let age_data = vec![30, 25, 35];

        use arrow_array::Int32Array;
        let name_col = Arc::new(StringArray::from(name_data.clone())) as ArrayRef;
        let age_col = Arc::new(Int32Array::from(age_data.clone())) as ArrayRef;

        let to_write = RecordBatch::try_new(arrow_schema.clone(), vec![name_col, age_col]).unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let file = File::create(format!("{table_location}/1.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, to_write.schema(), Some(props)).unwrap();

        writer.write(&to_write).expect("Writing batch");
        writer.close().unwrap();

        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();

        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask::builder()
                .with_file_size_in_bytes(
                    std::fs::metadata(format!("{table_location}/1.parquet"))
                        .unwrap()
                        .len(),
                )
                .with_start(0)
                .with_length(0)
                .with_data_file_path(format!("{table_location}/1.parquet"))
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(schema.clone())
                .with_project_field_ids(vec![1, 2])
                .with_case_sensitive(false)
                .build()
                .unwrap())]
            .into_iter(),
        )) as FileScanTaskStream;

        let result = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 2);

        // Verify position-based mapping: field_id 1 → position 0, field_id 2 → position 1
        let name_array = batch.column(0).as_string::<i32>();
        assert_eq!(name_array.value(0), "Alice");
        assert_eq!(name_array.value(1), "Bob");
        assert_eq!(name_array.value(2), "Charlie");

        let age_array = batch
            .column(1)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(age_array.value(0), 30);
        assert_eq!(age_array.value(1), 25);
        assert_eq!(age_array.value(2), 35);
    }

    /// Scan a field-id-less Parquet file (forcing the position-based fallback) built from
    /// `file_fields`/`file_columns`, projecting every field in `schema`.
    async fn read_migrated_file(
        file_fields: Vec<Field>,
        file_columns: Vec<ArrayRef>,
        schema: Arc<Schema>,
    ) -> Result<Vec<RecordBatch>, crate::Error> {
        // No field ID metadata on any field, and no name mapping on the task: branch 3.
        let arrow_schema = Arc::new(ArrowSchema::new(file_fields));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        let to_write = RecordBatch::try_new(arrow_schema, file_columns).unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let file = File::create(format!("{table_location}/1.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, to_write.schema(), Some(props)).unwrap();
        writer.write(&to_write).expect("Writing batch");
        writer.close().unwrap();

        let project_field_ids: Vec<i32> =
            schema.as_struct().fields().iter().map(|f| f.id).collect();

        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();
        let tasks = Box::pin(futures::stream::iter(vec![Ok(FileScanTask::builder()
            .with_file_size_in_bytes(
                std::fs::metadata(format!("{table_location}/1.parquet"))
                    .unwrap()
                    .len(),
            )
            .with_start(0)
            .with_length(0)
            .with_data_file_path(format!("{table_location}/1.parquet"))
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(schema)
            .with_project_field_ids(project_field_ids)
            .with_case_sensitive(false)
            .build()
            .unwrap())])) as FileScanTaskStream;

        reader.read(tasks).unwrap().stream().try_collect().await
    }

    /// Pins the known silent-NULL gap on the position-based fallback path, so that the
    /// behaviour is recorded rather than assumed absent. See the FOLLOW-UP comment in
    /// `get_arrow_projection_mask_fallback`.
    ///
    /// This path type-checks nothing, so the unreadable column *is* projected. It reaches
    /// `ColumnSource::Promote` in `RecordBatchTransformer` and is cast with `safe: true`, which
    /// turns unrepresentable values into NULL row by row while representable ones pass through
    /// untouched. A file `long` read as `int` therefore yields real values for small numbers and
    /// NULL for large ones -- harder to notice than a wholly-NULL column, not easier.
    ///
    /// Note that the field-id path errors on exactly this pair
    /// (`test_arrow_projection_mask_rejects_invalid_type_promotion`). The asymmetry is the gap.
    #[tokio::test]
    async fn test_fallback_projection_silently_nulls_unrepresentable_values() {
        use arrow_array::Int64Array;

        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "name", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::optional(2, "v", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        let batches = read_migrated_file(
            vec![
                Field::new("name", DataType::Utf8, false),
                Field::new("v", DataType::Int64, true),
            ],
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
                Arc::new(Int64Array::from(vec![30i64, 5_000_000_000, -3])) as ArrayRef,
            ],
            schema,
        )
        .await
        .expect("today the fallback path reads this file without complaint");

        let v = batches[0]
            .column(1)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(v.value(0), 30);
        // 5_000_000_000 does not fit in an `int`, and is dropped rather than reported.
        assert!(v.is_null(1), "expected the out-of-range value to be NULLed");
        assert_eq!(v.value(2), -3);
    }

    /// Files reaching the position-based fallback were not written by an Iceberg writer, so
    /// their physical types diverge from what an Iceberg writer would emit *by construction*:
    /// Hive/Impala write `string` as unannotated Parquet `binary`, and write a `timestamptz`
    /// column as a naive `Timestamp(µs, None)`. Both read correctly -- the cast is lossless --
    /// and both must keep reading correctly.
    ///
    /// This is a regression test for a rejected fix: pre-screening this path with
    /// `type_promotion_is_valid` (the field-id path's allowlist) turns both of these files into
    /// hard scan failures, because the allowlist answers "is this a legal Iceberg promotion",
    /// not "can arrow read this physical type losslessly". Any future fix for the gap pinned by
    /// `test_fallback_projection_silently_nulls_unrepresentable_values` must leave these two
    /// files readable.
    #[tokio::test]
    async fn test_fallback_projection_reads_diverging_physical_types() {
        use arrow_array::{BinaryArray, TimestampMicrosecondArray};

        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::optional(1, "s", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::optional(2, "ts", Type::Primitive(PrimitiveType::Timestamptz))
                        .into(),
                ])
                .build()
                .unwrap(),
        );

        let batches = read_migrated_file(
            vec![
                Field::new("s", DataType::Binary, true),
                Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            ],
            vec![
                Arc::new(BinaryArray::from(vec![&b"hello"[..], &b"world"[..]])) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(vec![
                    1_700_000_000_000_000i64,
                    1_700_000_001_000_000,
                ])) as ArrayRef,
            ],
            schema,
        )
        .await
        .expect("physical types that diverge losslessly must still read");

        let s = batches[0].column(0).as_string::<i32>();
        assert_eq!(s.null_count(), 0);
        assert_eq!(s.value(0), "hello");
        assert_eq!(s.value(1), "world");

        let ts = batches[0]
            .column(1)
            .as_primitive::<arrow_array::types::TimestampMicrosecondType>();
        assert_eq!(ts.null_count(), 0);
        assert_eq!(ts.value(0), 1_700_000_000_000_000);
        assert_eq!(
            batches[0].schema().field(1).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()))
        );
    }

    /// A spec-legal promotion (`int` -> `long`) must read on the fallback path too.
    #[tokio::test]
    async fn test_fallback_projection_allows_valid_promotion() {
        use arrow_array::Int32Array;

        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "name", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::optional(2, "age", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()
                .unwrap(),
        );

        let batches = read_migrated_file(
            vec![
                Field::new("name", DataType::Utf8, false),
                Field::new("age", DataType::Int32, true),
            ],
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
                Arc::new(Int32Array::from(vec![30, 25])) as ArrayRef,
            ],
            schema,
        )
        .await
        .expect("int -> long is a valid promotion and must still read");

        let age = batches[0]
            .column(1)
            .as_primitive::<arrow_array::types::Int64Type>();
        assert_eq!(age.null_count(), 0);
        assert_eq!(age.value(0), 30);
        assert_eq!(age.value(1), 25);
    }

    /// A column the filter reads must be type-checked even when it is not projected.
    ///
    /// `collect_scan_field_ids` derives `project_field_ids` from the selected column names
    /// alone, so before this was fixed `select(["name"]).filter(v < 100)` type-checked `name`
    /// and nothing else, while the row filter went on to read `v` from the file anyway. The
    /// same file that fails outright when `v` IS selected then returned rows filtered on values
    /// the projected schema says cannot exist -- the file's `v` is `long`, holding
    /// 5_000_000_000, which is not representable as the schema's `int`.
    ///
    /// Both directions are asserted here so the pair cannot drift apart again.
    #[tokio::test]
    async fn test_predicate_only_column_is_type_checked() {
        use arrow_array::Int64Array;

        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::optional(1, "name", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::optional(2, "v", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        let file_fields = vec![
            Field::new("name", DataType::Utf8, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
            Field::new("v", DataType::Int64, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )])),
        ];
        let file_columns = vec![
            Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![30i64, 5_000_000_000, -3])) as ArrayRef,
        ];

        let arrow_schema = Arc::new(ArrowSchema::new(file_fields));
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let to_write = RecordBatch::try_new(arrow_schema, file_columns).unwrap();
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let file = File::create(format!("{table_location}/1.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, to_write.schema(), Some(props)).unwrap();
        writer.write(&to_write).expect("Writing batch");
        writer.close().unwrap();

        let predicate = Reference::new("v")
            .less_than(Datum::int(100))
            .bind(schema.clone(), true)
            .unwrap();

        // `v` is filtered on but deliberately absent from the projection.
        for project_field_ids in [vec![1], vec![1, 2]] {
            let reader = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current()).build();
            let tasks = Box::pin(futures::stream::iter(vec![Ok(FileScanTask::builder()
                .with_file_size_in_bytes(
                    std::fs::metadata(format!("{table_location}/1.parquet"))
                        .unwrap()
                        .len(),
                )
                .with_start(0)
                .with_length(0)
                .with_data_file_path(format!("{table_location}/1.parquet"))
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(schema.clone())
                .with_project_field_ids(project_field_ids.clone())
                .with_case_sensitive(false)
                .with_predicate(Some(predicate.clone()))
                .build()
                .unwrap())])) as FileScanTaskStream;

            let err = reader
                .read(tasks)
                .unwrap()
                .stream()
                .try_collect::<Vec<RecordBatch>>()
                .await
                .expect_err(&format!(
                    "a filter column at an unreadable type must fail the scan whether or not it \
                     is projected (projection was {project_field_ids:?})"
                ));

            assert_eq!(err.kind(), ErrorKind::FeatureUnsupported, "{err}");
            assert!(
                err.to_string()
                    .contains("field 2 (v): file type long, projected type int"),
                "{err}"
            );
        }
    }

    /// Regression test for #2403: when a Parquet file lacks embedded field IDs but a
    /// name mapping is present, projection must use the field IDs assigned by the name
    /// mapping — not the position-based fallback (field_id N → column N-1).
    ///
    /// The scenario uses a file whose physical column order does not line up with the
    /// Iceberg field IDs: physical columns are `[name, subdept]` while the mapping
    /// assigns `name → 2` and `subdept → 4`. The position fallback would project
    /// field 2 from physical column 1 (`subdept`) and drop field 4 (column 3 is out of
    /// range), silently misreading data.
    #[tokio::test]
    async fn test_read_parquet_with_name_mapping_uses_mapped_field_ids() {
        // Iceberg schema: physical file order does NOT match field-id order.
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::optional(3, "dept", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::optional(4, "subdept", Type::Primitive(PrimitiveType::String))
                        .into(),
                ])
                .build()
                .unwrap(),
        );

        // Migrated Parquet file: no field-id metadata, physical columns [name, subdept].
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("subdept", DataType::Utf8, true),
        ]));

        let name_mapping = Arc::new(NameMapping::new(vec![
            MappedField::new(Some(2), vec!["name".to_string()], vec![]),
            MappedField::new(Some(4), vec!["subdept".to_string()], vec![]),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        let name_col = Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])) as ArrayRef;
        let subdept_col = Arc::new(StringArray::from(vec!["comms", "tax", "audit"])) as ArrayRef;

        let to_write =
            RecordBatch::try_new(arrow_schema.clone(), vec![name_col, subdept_col]).unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let file = File::create(format!("{table_location}/1.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, to_write.schema(), Some(props)).unwrap();
        writer.write(&to_write).expect("Writing batch");
        writer.close().unwrap();

        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();

        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask::builder()
                .with_file_size_in_bytes(
                    std::fs::metadata(format!("{table_location}/1.parquet"))
                        .unwrap()
                        .len(),
                )
                .with_start(0)
                .with_length(0)
                .with_data_file_path(format!("{table_location}/1.parquet"))
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(schema.clone())
                .with_project_field_ids(vec![2, 4])
                .with_case_sensitive(false)
                .with_name_mapping(Some(name_mapping))
                .build()
                .unwrap())]
            .into_iter(),
        )) as FileScanTaskStream;

        let result = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 2);

        // field 2 (`name`) must come from physical column 0, not be NULL-filled.
        let name_array = batch.column(0).as_string::<i32>();
        assert_eq!(
            name_array.null_count(),
            0,
            "`name` was NULL-filled: name mapping was ignored and position fallback was used"
        );
        assert_eq!(name_array.value(0), "Alice");
        assert_eq!(name_array.value(1), "Bob");
        assert_eq!(name_array.value(2), "Charlie");

        // field 4 (`subdept`) must come from physical column 1.
        let subdept_array = batch.column(1).as_string::<i32>();
        assert_eq!(subdept_array.null_count(), 0);
        assert_eq!(subdept_array.value(0), "comms");
        assert_eq!(subdept_array.value(1), "tax");
        assert_eq!(subdept_array.value(2), "audit");
    }

    /// Regression test for #2403, predicate side: with a name mapping present, predicate
    /// pushdown must resolve field IDs via the mapping rather than the position fallback,
    /// which would evaluate the filter against the wrong physical column.
    #[tokio::test]
    async fn test_predicate_on_name_mapped_file_uses_mapped_field_ids() {
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::optional(3, "dept", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::optional(4, "subdept", Type::Primitive(PrimitiveType::String))
                        .into(),
                ])
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("subdept", DataType::Utf8, true),
        ]));

        let name_mapping = Arc::new(NameMapping::new(vec![
            MappedField::new(Some(2), vec!["name".to_string()], vec![]),
            MappedField::new(Some(4), vec!["subdept".to_string()], vec![]),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        // Engineered so that filtering the wrong physical column yields different rows:
        // `name` and `subdept` both contain the value "Alice", on different rows.
        let name_col = Arc::new(StringArray::from(vec!["Alice", "Bob", "Sue"])) as ArrayRef;
        let subdept_col = Arc::new(StringArray::from(vec!["Bob", "Alice", "Alice"])) as ArrayRef;

        let to_write =
            RecordBatch::try_new(arrow_schema.clone(), vec![name_col, subdept_col]).unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let file = File::create(format!("{table_location}/1.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, to_write.schema(), Some(props)).unwrap();
        writer.write(&to_write).expect("Writing batch");
        writer.close().unwrap();

        let predicate = Reference::new("name").equal_to(Datum::string("Alice"));

        let reader = ArrowReaderBuilder::new(file_io, Runtime::current())
            .with_row_group_filtering_enabled(true)
            .with_row_selection_enabled(true)
            .build();

        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask::builder()
                .with_file_size_in_bytes(
                    std::fs::metadata(format!("{table_location}/1.parquet"))
                        .unwrap()
                        .len(),
                )
                .with_start(0)
                .with_length(0)
                .with_data_file_path(format!("{table_location}/1.parquet"))
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(schema.clone())
                .with_project_field_ids(vec![2, 4])
                .with_case_sensitive(false)
                .with_name_mapping(Some(name_mapping))
                .with_predicate(Some(predicate.bind(schema, true).unwrap()))
                .build()
                .unwrap())]
            .into_iter(),
        )) as FileScanTaskStream;

        let result = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 1,
            "filter `name = \"Alice\"` matched the wrong rows: predicate was evaluated \
             against the wrong physical column"
        );

        let batch = &result[0];
        let name_array = batch.column(0).as_string::<i32>();
        assert_eq!(name_array.value(0), "Alice");
        let subdept_array = batch.column(1).as_string::<i32>();
        assert_eq!(subdept_array.value(0), "Bob");
    }

    /// Test reading Parquet files without field IDs with partial projection.
    /// Only a subset of columns are requested, verifying position-based fallback
    /// handles column selection correctly.
    #[tokio::test]
    async fn test_read_parquet_without_field_ids_partial_projection() {
        use arrow_array::Int32Array;

        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "col1", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(2, "col2", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(3, "col3", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(4, "col4", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("col1", DataType::Utf8, false),
            Field::new("col2", DataType::Int32, false),
            Field::new("col3", DataType::Utf8, false),
            Field::new("col4", DataType::Int32, false),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        let col1_data = Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef;
        let col2_data = Arc::new(Int32Array::from(vec![10, 20])) as ArrayRef;
        let col3_data = Arc::new(StringArray::from(vec!["c", "d"])) as ArrayRef;
        let col4_data = Arc::new(Int32Array::from(vec![30, 40])) as ArrayRef;

        let to_write = RecordBatch::try_new(arrow_schema.clone(), vec![
            col1_data, col2_data, col3_data, col4_data,
        ])
        .unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let file = File::create(format!("{table_location}/1.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, to_write.schema(), Some(props)).unwrap();

        writer.write(&to_write).expect("Writing batch");
        writer.close().unwrap();

        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();

        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask::builder()
                .with_file_size_in_bytes(
                    std::fs::metadata(format!("{table_location}/1.parquet"))
                        .unwrap()
                        .len(),
                )
                .with_start(0)
                .with_length(0)
                .with_data_file_path(format!("{table_location}/1.parquet"))
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(schema.clone())
                .with_project_field_ids(vec![1, 3])
                .with_case_sensitive(false)
                .build()
                .unwrap())]
            .into_iter(),
        )) as FileScanTaskStream;

        let result = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);

        let col1_array = batch.column(0).as_string::<i32>();
        assert_eq!(col1_array.value(0), "a");
        assert_eq!(col1_array.value(1), "b");

        let col3_array = batch.column(1).as_string::<i32>();
        assert_eq!(col3_array.value(0), "c");
        assert_eq!(col3_array.value(1), "d");
    }

    /// Test reading Parquet files without field IDs with schema evolution.
    /// The Iceberg schema has more fields than the Parquet file, testing that
    /// missing columns are filled with NULLs.
    #[tokio::test]
    async fn test_read_parquet_without_field_ids_schema_evolution() {
        use arrow_array::{Array, Int32Array};

        // Schema with field 3 added after the file was written
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "name", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(2, "age", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(3, "city", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, false),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        let name_data = Arc::new(StringArray::from(vec!["Alice", "Bob"])) as ArrayRef;
        let age_data = Arc::new(Int32Array::from(vec![30, 25])) as ArrayRef;

        let to_write =
            RecordBatch::try_new(arrow_schema.clone(), vec![name_data, age_data]).unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let file = File::create(format!("{table_location}/1.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, to_write.schema(), Some(props)).unwrap();

        writer.write(&to_write).expect("Writing batch");
        writer.close().unwrap();

        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();

        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask::builder()
                .with_file_size_in_bytes(
                    std::fs::metadata(format!("{table_location}/1.parquet"))
                        .unwrap()
                        .len(),
                )
                .with_start(0)
                .with_length(0)
                .with_data_file_path(format!("{table_location}/1.parquet"))
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(schema.clone())
                .with_project_field_ids(vec![1, 2, 3])
                .with_case_sensitive(false)
                .build()
                .unwrap())]
            .into_iter(),
        )) as FileScanTaskStream;

        let result = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);

        let name_array = batch.column(0).as_string::<i32>();
        assert_eq!(name_array.value(0), "Alice");
        assert_eq!(name_array.value(1), "Bob");

        let age_array = batch
            .column(1)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(age_array.value(0), 30);
        assert_eq!(age_array.value(1), 25);

        // Verify missing column filled with NULLs
        let city_array = batch.column(2).as_string::<i32>();
        assert_eq!(city_array.null_count(), 2);
        assert!(city_array.is_null(0));
        assert!(city_array.is_null(1));
    }

    /// Test reading Parquet files without field IDs that have multiple row groups.
    /// This ensures the position-based fallback works correctly across row group boundaries.
    #[tokio::test]
    async fn test_read_parquet_without_field_ids_multiple_row_groups() {
        use arrow_array::Int32Array;

        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "name", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(2, "value", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        // Small row group size to create multiple row groups
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_write_batch_size(2)
            .set_max_row_group_row_count(Some(2))
            .build();

        let file = File::create(format!("{table_location}/1.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(props)).unwrap();

        // Write 6 rows in 3 batches (will create 3 row groups)
        for batch_num in 0..3 {
            let name_data = Arc::new(StringArray::from(vec![
                format!("name_{}", batch_num * 2),
                format!("name_{}", batch_num * 2 + 1),
            ])) as ArrayRef;
            let value_data =
                Arc::new(Int32Array::from(vec![batch_num * 2, batch_num * 2 + 1])) as ArrayRef;

            let batch =
                RecordBatch::try_new(arrow_schema.clone(), vec![name_data, value_data]).unwrap();
            writer.write(&batch).expect("Writing batch");
        }
        writer.close().unwrap();

        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();

        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask::builder()
                .with_file_size_in_bytes(
                    std::fs::metadata(format!("{table_location}/1.parquet"))
                        .unwrap()
                        .len(),
                )
                .with_start(0)
                .with_length(0)
                .with_data_file_path(format!("{table_location}/1.parquet"))
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(schema.clone())
                .with_project_field_ids(vec![1, 2])
                .with_case_sensitive(false)
                .build()
                .unwrap())]
            .into_iter(),
        )) as FileScanTaskStream;

        let result = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        assert!(!result.is_empty());

        let mut all_names = Vec::new();
        let mut all_values = Vec::new();

        for batch in &result {
            let name_array = batch.column(0).as_string::<i32>();
            let value_array = batch
                .column(1)
                .as_primitive::<arrow_array::types::Int32Type>();

            for i in 0..batch.num_rows() {
                all_names.push(name_array.value(i).to_string());
                all_values.push(value_array.value(i));
            }
        }

        assert_eq!(all_names.len(), 6);
        assert_eq!(all_values.len(), 6);

        for i in 0..6 {
            assert_eq!(all_names[i], format!("name_{i}"));
            assert_eq!(all_values[i], i as i32);
        }
    }

    /// Test reading Parquet files without field IDs with nested types (struct).
    /// Java's pruneColumnsFallback() projects entire top-level columns including nested content.
    /// This test verifies that a top-level struct field is projected correctly with all its nested fields.
    #[tokio::test]
    async fn test_read_parquet_without_field_ids_with_struct() {
        use arrow_array::{Int32Array, StructArray};
        use arrow_schema::Fields;

        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(
                        2,
                        "person",
                        Type::Struct(StructType::new(vec![
                            NestedField::required(
                                3,
                                "name",
                                Type::Primitive(PrimitiveType::String),
                            )
                            .into(),
                            NestedField::required(4, "age", Type::Primitive(PrimitiveType::Int))
                                .into(),
                        ])),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "person",
                DataType::Struct(Fields::from(vec![
                    Field::new("name", DataType::Utf8, false),
                    Field::new("age", DataType::Int32, false),
                ])),
                false,
            ),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        let id_data = Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef;
        let name_data = Arc::new(StringArray::from(vec!["Alice", "Bob"])) as ArrayRef;
        let age_data = Arc::new(Int32Array::from(vec![30, 25])) as ArrayRef;
        let person_data = Arc::new(StructArray::from(vec![
            (
                Arc::new(Field::new("name", DataType::Utf8, false)),
                name_data,
            ),
            (
                Arc::new(Field::new("age", DataType::Int32, false)),
                age_data,
            ),
        ])) as ArrayRef;

        let to_write =
            RecordBatch::try_new(arrow_schema.clone(), vec![id_data, person_data]).unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let file = File::create(format!("{table_location}/1.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, to_write.schema(), Some(props)).unwrap();

        writer.write(&to_write).expect("Writing batch");
        writer.close().unwrap();

        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();

        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask::builder()
                .with_file_size_in_bytes(
                    std::fs::metadata(format!("{table_location}/1.parquet"))
                        .unwrap()
                        .len(),
                )
                .with_start(0)
                .with_length(0)
                .with_data_file_path(format!("{table_location}/1.parquet"))
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(schema.clone())
                .with_project_field_ids(vec![1, 2])
                .with_case_sensitive(false)
                .build()
                .unwrap())]
            .into_iter(),
        )) as FileScanTaskStream;

        let result = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);

        let id_array = batch
            .column(0)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(id_array.value(0), 1);
        assert_eq!(id_array.value(1), 2);

        let person_array = batch.column(1).as_struct();
        assert_eq!(person_array.num_columns(), 2);

        let name_array = person_array.column(0).as_string::<i32>();
        assert_eq!(name_array.value(0), "Alice");
        assert_eq!(name_array.value(1), "Bob");

        let age_array = person_array
            .column(1)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(age_array.value(0), 30);
        assert_eq!(age_array.value(1), 25);
    }

    /// Test reading Parquet files without field IDs with schema evolution - column added in the middle.
    /// When a new column is inserted between existing columns in the schema order,
    /// the fallback projection must correctly map field IDs to output positions.
    #[tokio::test]
    async fn test_read_parquet_without_field_ids_schema_evolution_add_column_in_middle() {
        use arrow_array::{Array, Int32Array};

        let arrow_schema_old = Arc::new(ArrowSchema::new(vec![
            Field::new("col0", DataType::Int32, true),
            Field::new("col1", DataType::Int32, true),
        ]));

        // New column added between existing columns: col0 (id=1), newCol (id=5), col1 (id=2)
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::optional(1, "col0", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(5, "newCol", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(2, "col1", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        let col0_data = Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef;
        let col1_data = Arc::new(Int32Array::from(vec![10, 20])) as ArrayRef;

        let to_write =
            RecordBatch::try_new(arrow_schema_old.clone(), vec![col0_data, col1_data]).unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let file = File::create(format!("{table_location}/1.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, to_write.schema(), Some(props)).unwrap();
        writer.write(&to_write).expect("Writing batch");
        writer.close().unwrap();

        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();

        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask::builder()
                .with_file_size_in_bytes(
                    std::fs::metadata(format!("{table_location}/1.parquet"))
                        .unwrap()
                        .len(),
                )
                .with_start(0)
                .with_length(0)
                .with_data_file_path(format!("{table_location}/1.parquet"))
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(schema.clone())
                .with_project_field_ids(vec![1, 5, 2])
                .with_case_sensitive(false)
                .build()
                .unwrap())]
            .into_iter(),
        )) as FileScanTaskStream;

        let result = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);

        let result_col0 = batch
            .column(0)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(result_col0.value(0), 1);
        assert_eq!(result_col0.value(1), 2);

        // New column should be NULL (doesn't exist in old file)
        let result_newcol = batch
            .column(1)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(result_newcol.null_count(), 2);
        assert!(result_newcol.is_null(0));
        assert!(result_newcol.is_null(1));

        let result_col1 = batch
            .column(2)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(result_col1.value(0), 10);
        assert_eq!(result_col1.value(1), 20);
    }

    /// Test reading Parquet files without field IDs with a filter that eliminates all row groups.
    /// During development of field ID mapping, we saw a panic when row_selection_enabled=true and
    /// all row groups are filtered out.
    #[tokio::test]
    async fn test_read_parquet_without_field_ids_filter_eliminates_all_rows() {
        use arrow_array::{Float64Array, Int32Array};

        // Schema with fields that will use fallback IDs 1, 2, 3
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(3, "value", Type::Primitive(PrimitiveType::Double))
                        .into(),
                ])
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        // Write data where all ids are >= 10
        let id_data = Arc::new(Int32Array::from(vec![10, 11, 12])) as ArrayRef;
        let name_data = Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef;
        let value_data = Arc::new(Float64Array::from(vec![100.0, 200.0, 300.0])) as ArrayRef;

        let to_write =
            RecordBatch::try_new(arrow_schema.clone(), vec![id_data, name_data, value_data])
                .unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let file = File::create(format!("{table_location}/1.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, to_write.schema(), Some(props)).unwrap();
        writer.write(&to_write).expect("Writing batch");
        writer.close().unwrap();

        // Filter that eliminates all row groups: id < 5
        let predicate = Reference::new("id").less_than(Datum::int(5));

        // Enable both row_group_filtering and row_selection - triggered the panic
        let reader = ArrowReaderBuilder::new(file_io, Runtime::current())
            .with_row_group_filtering_enabled(true)
            .with_row_selection_enabled(true)
            .build();

        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask::builder()
                .with_file_size_in_bytes(
                    std::fs::metadata(format!("{table_location}/1.parquet"))
                        .unwrap()
                        .len(),
                )
                .with_start(0)
                .with_length(0)
                .with_data_file_path(format!("{table_location}/1.parquet"))
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(schema.clone())
                .with_project_field_ids(vec![1, 2, 3])
                .with_case_sensitive(false)
                .with_predicate(Some(predicate.bind(schema, true).unwrap()))
                .build()
                .unwrap())]
            .into_iter(),
        )) as FileScanTaskStream;

        // Should no longer panic
        let result = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        // Should return empty results
        assert!(result.is_empty() || result.iter().all(|batch| batch.num_rows() == 0));
    }

    /// Test bucket partitioning reads source column from data file (not partition metadata).
    ///
    /// This is an integration test verifying the complete ArrowReader pipeline with bucket partitioning.
    /// It corresponds to TestRuntimeFiltering tests in Iceberg Java (e.g., testRenamedSourceColumnTable).
    ///
    /// # Iceberg Spec Requirements
    ///
    /// Per the Iceberg spec "Column Projection" section:
    /// > "Return the value from partition metadata if an **Identity Transform** exists for the field"
    ///
    /// This means:
    /// - Identity transforms (e.g., `identity(dept)`) use constants from partition metadata
    /// - Non-identity transforms (e.g., `bucket(4, id)`) must read source columns from data files
    /// - Partition metadata for bucket transforms stores bucket numbers (0-3), NOT source values
    ///
    /// Java's PartitionUtil.constantsMap() implements this via:
    /// ```java
    /// if (field.transform().isIdentity()) {
    ///     idToConstant.put(field.sourceId(), converted);
    /// }
    /// ```
    ///
    /// # What This Test Verifies
    ///
    /// This test ensures the full ArrowReader → RecordBatchTransformer pipeline correctly handles
    /// bucket partitioning when FileScanTask provides partition_spec and partition_data:
    ///
    /// - Parquet file has field_id=1 named "id" with actual data [1, 5, 9, 13]
    /// - FileScanTask specifies partition_spec with bucket(4, id) and partition_data with bucket=1
    /// - RecordBatchTransformer.constants_map() excludes bucket-partitioned field from constants
    /// - ArrowReader correctly reads [1, 5, 9, 13] from the data file
    /// - Values are NOT replaced with constant 1 from partition metadata
    ///
    /// # Why This Matters
    ///
    /// Without correct handling:
    /// - Runtime filtering would break (e.g., `WHERE id = 5` would fail)
    /// - Query results would be incorrect (all rows would have id=1)
    /// - Bucket partitioning would be unusable for query optimization
    ///
    /// # References
    /// - Iceberg spec: format/spec.md "Column Projection" + "Partition Transforms"
    /// - Java test: spark/src/test/java/.../TestRuntimeFiltering.java
    /// - Java impl: core/src/main/java/org/apache/iceberg/util/PartitionUtil.java
    #[tokio::test]
    async fn test_bucket_partitioning_reads_source_column_from_file() {
        use arrow_array::Int32Array;

        use crate::spec::{Literal, PartitionSpec, Struct, Transform};

        // Iceberg schema with id and name columns
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(0)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .unwrap(),
        );

        // Partition spec: bucket(4, id)
        let partition_spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .with_spec_id(0)
                .add_partition_field("id", "id_bucket", Transform::Bucket(4))
                .unwrap()
                .build()
                .unwrap(),
        );

        // Partition data: bucket value is 1
        let partition_data = Struct::from_iter(vec![Some(Literal::int(1))]);

        // Create Arrow schema with field IDs for Parquet file
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
            Field::new("name", DataType::Utf8, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )])),
        ]));

        // Write Parquet file with data
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        let id_data = Arc::new(Int32Array::from(vec![1, 5, 9, 13])) as ArrayRef;
        let name_data =
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie", "Dave"])) as ArrayRef;

        let to_write =
            RecordBatch::try_new(arrow_schema.clone(), vec![id_data, name_data]).unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let file = File::create(format!("{}/data.parquet", &table_location)).unwrap();
        let mut writer = ArrowWriter::try_new(file, to_write.schema(), Some(props)).unwrap();
        writer.write(&to_write).expect("Writing batch");
        writer.close().unwrap();

        // Read the Parquet file with partition spec and data
        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();
        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask::builder()
                .with_file_size_in_bytes(
                    std::fs::metadata(format!("{table_location}/data.parquet"))
                        .unwrap()
                        .len(),
                )
                .with_start(0)
                .with_length(0)
                .with_data_file_path(format!("{table_location}/data.parquet"))
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(schema.clone())
                .with_project_field_ids(vec![1, 2])
                .with_case_sensitive(false)
                .with_partition(Some(partition_data))
                .with_partition_spec(Some(partition_spec))
                .build()
                .unwrap())]
            .into_iter(),
        )) as FileScanTaskStream;

        let result = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        // Verify we got the correct data
        assert_eq!(result.len(), 1);
        let batch = &result[0];

        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.num_rows(), 4);

        // The id column MUST contain actual values from the Parquet file [1, 5, 9, 13],
        // NOT the constant partition value 1
        let id_col = batch
            .column(0)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(id_col.value(0), 1);
        assert_eq!(id_col.value(1), 5);
        assert_eq!(id_col.value(2), 9);
        assert_eq!(id_col.value(3), 13);

        let name_col = batch.column(1).as_string::<i32>();
        assert_eq!(name_col.value(0), "Alice");
        assert_eq!(name_col.value(1), "Bob");
        assert_eq!(name_col.value(2), "Charlie");
        assert_eq!(name_col.value(3), "Dave");
    }

    /// An identity-partitioned column that IS in the data file at an unreadable type must
    /// fail the scan -- it must NOT fall back to the partition-metadata constant.
    ///
    /// This pins a deliberate behaviour change, so it is worth being explicit about why the
    /// old behaviour was wrong rather than merely different.
    ///
    /// Column-projection rule #1 ("Return the value from partition metadata if an Identity
    /// Transform exists for the field") reads, in the spec, under a preamble that scopes all
    /// four rules: *"Values for field ids which are **not present in a data file** must be
    /// resolved according the following rules"*. Rule #1 is therefore a rule about absent
    /// columns -- it exists for metadata-only Hive migrations, where the partition column
    /// lives in the directory path and not in the file. It says nothing about a column the
    /// file does contain. `RecordBatchTransformer::generate_transform_operations` already
    /// encodes exactly this: an identity-partition field present in the file falls through
    /// to be read from the file instead of using the constant.
    ///
    /// Before this change the constant was served here anyway, but not because rule #1
    /// applied -- because two conflations stacked. Projection skipped the unpromotable leaf,
    /// which made the transformer's `present_in_file` check (which consults the *projected*
    /// batch, not the file) see an absent column, which handed it to rule #1. The right
    /// answer arrived, if at all, by accident.
    ///
    /// It is also not reliably the right answer. For an identity partition the constant
    /// equals the column's value for every row *if the file agrees with its partition
    /// metadata* -- and we cannot check that, because the column is precisely the one we
    /// cannot decode. A file whose `dt` disagrees with the manifest (a mis-written
    /// `add_files` migration, say) would have its disagreement papered over by the very
    /// constant we substituted. That is the same defect this commit removes: answering
    /// confidently from a guess about a type we do not understand. Failing is correct.
    #[tokio::test]
    async fn test_identity_partition_column_unreadable_in_file_errors() {
        use arrow_array::{Int32Array, Int64Array};

        use crate::spec::{Literal, PartitionSpec, Struct, Transform};

        // Schema declares `dt` as Int; the file stores it as Int64. Narrowing Long -> Int is
        // not a valid promotion, so `dt` is present in the file but unreadable as projected.
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(0)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "dt", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        // identity(dt): rule #1 would supply a constant for `dt` if it were absent.
        let partition_spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .with_spec_id(0)
                .add_partition_field("dt", "dt", Transform::Identity)
                .unwrap()
                .build()
                .unwrap(),
        );
        let partition_data = Struct::from_iter(vec![Some(Literal::int(20240101))]);

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
            Field::new("dt", DataType::Int64, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )])),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        let id_data = Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef;
        // Deliberately disagrees with the partition metadata above, which is only
        // observable if the file column is actually read.
        let dt_data = Arc::new(Int64Array::from(vec![20240102, 20240103])) as ArrayRef;

        let to_write = RecordBatch::try_new(arrow_schema.clone(), vec![id_data, dt_data]).unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let file = File::create(format!("{table_location}/data.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, to_write.schema(), Some(props)).unwrap();
        writer.write(&to_write).expect("Writing batch");
        writer.close().unwrap();

        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();
        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask::builder()
                .with_file_size_in_bytes(
                    std::fs::metadata(format!("{table_location}/data.parquet"))
                        .unwrap()
                        .len(),
                )
                .with_start(0)
                .with_length(0)
                .with_data_file_path(format!("{table_location}/data.parquet"))
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(schema.clone())
                .with_project_field_ids(vec![1, 2])
                .with_case_sensitive(false)
                .with_partition(Some(partition_data))
                .with_partition_spec(Some(partition_spec))
                .build()
                .unwrap())]
            .into_iter(),
        )) as FileScanTaskStream;

        let err = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .expect_err(
                "an identity-partitioned column present in the file at an unreadable type must \
                 fail the scan, not silently resolve to the partition-metadata constant",
            );

        assert_eq!(err.kind(), ErrorKind::FeatureUnsupported, "{err}");
        assert!(
            err.to_string()
                .contains("field 2 (dt): file type long, projected type int"),
            "{err}"
        );
    }

    /// Regression for <https://github.com/apache/iceberg-rust/issues/2306>:
    /// predicate on a column after nested types in a migrated file (no field IDs).
    /// Schema has struct, list, and map columns before the predicate target (`id`),
    /// exercising the fallback field ID mapping across all nested type variants.
    #[tokio::test]
    async fn test_predicate_on_migrated_file_with_nested_types() {
        use serde::{Deserialize, Serialize};
        use serde_arrow::schema::{SchemaLike, TracingOptions};

        #[derive(Serialize, Deserialize)]
        struct Person {
            name: String,
            age: i32,
        }

        #[derive(Serialize, Deserialize)]
        struct Row {
            person: Person,
            people: Vec<Person>,
            props: std::collections::BTreeMap<String, String>,
            id: i32,
        }

        let rows = vec![
            Row {
                person: Person {
                    name: "Alice".into(),
                    age: 30,
                },
                people: vec![Person {
                    name: "Alice".into(),
                    age: 30,
                }],
                props: [("k1".into(), "v1".into())].into(),
                id: 1,
            },
            Row {
                person: Person {
                    name: "Bob".into(),
                    age: 25,
                },
                people: vec![Person {
                    name: "Bob".into(),
                    age: 25,
                }],
                props: [("k2".into(), "v2".into())].into(),
                id: 2,
            },
            Row {
                person: Person {
                    name: "Carol".into(),
                    age: 40,
                },
                people: vec![Person {
                    name: "Carol".into(),
                    age: 40,
                }],
                props: [("k3".into(), "v3".into())].into(),
                id: 3,
            },
        ];

        let tracing_options = TracingOptions::default()
            .map_as_struct(false)
            .strings_as_large_utf8(false)
            .sequence_as_large_list(false);
        let fields = Vec::<arrow_schema::FieldRef>::from_type::<Row>(tracing_options).unwrap();
        let arrow_schema = Arc::new(ArrowSchema::new(fields.clone()));
        let batch = serde_arrow::to_record_batch(&fields, &rows).unwrap();

        // Fallback field IDs: person=1, people=2, props=3, id=4
        let iceberg_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(
                        1,
                        "person",
                        Type::Struct(StructType::new(vec![
                            NestedField::required(
                                5,
                                "name",
                                Type::Primitive(PrimitiveType::String),
                            )
                            .into(),
                            NestedField::required(6, "age", Type::Primitive(PrimitiveType::Int))
                                .into(),
                        ])),
                    )
                    .into(),
                    NestedField::required(
                        2,
                        "people",
                        Type::List(crate::spec::ListType {
                            element_field: NestedField::required(
                                7,
                                "element",
                                Type::Struct(StructType::new(vec![
                                    NestedField::required(
                                        8,
                                        "name",
                                        Type::Primitive(PrimitiveType::String),
                                    )
                                    .into(),
                                    NestedField::required(
                                        9,
                                        "age",
                                        Type::Primitive(PrimitiveType::Int),
                                    )
                                    .into(),
                                ])),
                            )
                            .into(),
                        }),
                    )
                    .into(),
                    NestedField::required(
                        3,
                        "props",
                        Type::Map(crate::spec::MapType {
                            key_field: NestedField::required(
                                10,
                                "key",
                                Type::Primitive(PrimitiveType::String),
                            )
                            .into(),
                            value_field: NestedField::required(
                                11,
                                "value",
                                Type::Primitive(PrimitiveType::String),
                            )
                            .into(),
                        }),
                    )
                    .into(),
                    NestedField::required(4, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_path = format!("{table_location}/1.parquet");

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let file = File::create(&file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, arrow_schema, Some(props)).unwrap();
        writer.write(&batch).expect("Writing batch");
        writer.close().unwrap();

        let predicate = Reference::new("id").greater_than(Datum::int(1));

        let reader = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current())
            .with_row_group_filtering_enabled(true)
            .with_row_selection_enabled(true)
            .build();

        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask::builder()
                .with_file_size_in_bytes(std::fs::metadata(&file_path).unwrap().len())
                .with_start(0)
                .with_length(0)
                .with_data_file_path(file_path)
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(iceberg_schema.clone())
                .with_project_field_ids(vec![4])
                .with_case_sensitive(false)
                .with_predicate(Some(predicate.bind(iceberg_schema, true).unwrap()))
                .build()
                .unwrap())]
            .into_iter(),
        )) as FileScanTaskStream;

        let result = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        let ids: Vec<i32> = result
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_primitive::<arrow_array::types::Int32Type>()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        assert_eq!(ids, vec![2, 3]);
    }
}
