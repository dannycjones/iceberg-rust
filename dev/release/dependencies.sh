#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -Eeuo pipefail

# bash 3.2 (macOS default) does not run the ERR trap for a failing subshell:
# the script still exits on failure but does not report which step failed.
if [ "${BASH_VERSINFO[0]}" -lt 4 ]; then
  echo "Warning: bash ${BASH_VERSION} will not print which step failed. Use bash 4 or newer to see it." >&2
fi

# Keep this in sync with CARGO_DENY_VERSION in .github/workflows/dependencies.yml.
# The generated DEPENDENCIES.rust.tsv files are version-sensitive, so the local
# cargo-deny must match the version CI uses to avoid spurious diffs.
EXPECTED_CARGO_DENY_VERSION="0.19.9"

# Keep this in sync with the cargo-about version installed by the
# third-party-licenses job in each Python wheel workflow, and with the
# install-cargo-about target in Makefile. The rendered bundle is what ships in the
# wheels, so a locally generated copy should match what CI produces.
EXPECTED_CARGO_ABOUT_VERSION="0.8.4"

# Name of the generated license-text bundle. It is a build output, not a
# checked-in file, and is gitignored.
THIRD_PARTY_LICENSES_FILE="THIRD-PARTY-LICENSES"

# Directory, relative to a package, holding one subdirectory of generated legal
# files per published wheel. Gitignored, and never staged into a source release.
LICENSES_DIR_NAME="licenses"

# Heading that introduces the relayed dependency notices in the generated NOTICE.
# dev/check_wheel_licenses.py greps built wheels for it, so keep them in sync.
NOTICE_RELAY_HEADING="Bundled dependency notices"

# Heading that introduces the pointer to ${THIRD_PARTY_LICENSES_FILE} appended to
# the generated LICENSE. dev/check_wheel_licenses.py greps for it too.
LICENSE_APPENDIX_HEADING="Bundled third-party components"

# One line per published wheel: the report name, a tab, then the Rust target
# triples that wheel's extension module is linked for, space separated.
#
# A report covers exactly the crates its triples resolve to, so each wheel carries
# attribution for what it actually contains and nothing more. The ASF is explicit
# that LICENSE and NOTICE "must exactly represent the contents of the distribution
# they reside in" and that dependencies not in the distribution must not be added
# to them, which a single union report cannot satisfy.
#
# The names are the contract with CI: each must match a `license_target` value in
# the Python wheel workflow matrices, and the triples must match what maturin
# builds for that wheel. universal2 is a fat binary, so it links both Apple
# triples and its report is the union of the two. The macOS CI test job builds a
# host-only wheel, which is why aarch64-apple-darwin also appears alone.
WHEEL_LICENSE_REPORTS="$(
  cat <<'REPORTS'
x86_64-unknown-linux-gnu	x86_64-unknown-linux-gnu
aarch64-unknown-linux-gnu	aarch64-unknown-linux-gnu
armv7-unknown-linux-gnueabihf	armv7-unknown-linux-gnueabihf
universal2-apple-darwin	x86_64-apple-darwin aarch64-apple-darwin
aarch64-apple-darwin	aarch64-apple-darwin
x86_64-pc-windows-msvc	x86_64-pc-windows-msvc
REPORTS
)"

CURRENT_STEP=""

on_error() {
  local status=$?
  if [ -n "${CURRENT_STEP}" ]; then
    echo "FAILED: ${CURRENT_STEP}" >&2
  else
    echo "FAILED" >&2
  fi
  exit "${status}"
}

trap on_error ERR

usage() {
  cat <<USAGE
Usage:
  $0 <check|generate|generate-third-party-licenses>
  $0 stage-third-party-licenses <report>

Commands:
  check
      Run cargo-deny license validation once at the root workspace.
      Default command: none; this argument is required.

  generate
      Regenerate each workspace package's checked-in DEPENDENCIES.rust.tsv file.
      Package directories are discovered from cargo metadata.
      Default command: none; this argument is required.

  generate-third-party-licenses
      For every package that declares an about.toml, write one set of legal files
      per published wheel into ${LICENSES_DIR_NAME}/<report>/: a
      ${THIRD_PARTY_LICENSES_FILE} bundle of full dependency license texts, a
      NOTICE that also relays the NOTICE files of the bundled dependencies, and a
      LICENSE that points at the bundle. Each set covers only the crates that
      wheel's target triples resolve to. Nothing is staged for packaging; use
      stage-third-party-licenses for that. The output is gitignored.
      Default command: none; this argument is required.

  stage-third-party-licenses <report>
      Copy the ${LICENSES_DIR_NAME}/<report>/ files over each package's LICENSE,
      NOTICE, and ${THIRD_PARTY_LICENSES_FILE} so maturin packages them. The wheel
      workflows run this after downloading the generated reports and before
      maturin. LICENSE and NOTICE are checked-in symlinks, so this leaves the
      worktree dirty -- restore them with
      'git checkout -- <package>/LICENSE <package>/NOTICE'.
      Reports:
$(echo "${WHEEL_LICENSE_REPORTS}" | cut -f1 | sed 's|^|        |')

Options:
  -h, --help
      Show this help message.

Examples:
  $0 check
  $0 generate
  $0 generate-third-party-licenses
  $0 stage-third-party-licenses x86_64-unknown-linux-gnu
USAGE
}

show_help_if_requested() {
  while [ "$#" -gt 0 ]; do
    case "$1" in
      -h | --help)
        usage
        exit 0
        ;;
    esac
    shift
  done
}

start_step() {
  CURRENT_STEP="$1"
  echo "==> ${CURRENT_STEP}"
}

finish_step() {
  echo "OK: ${CURRENT_STEP}"
  CURRENT_STEP=""
}

run_step() {
  local step="$1"
  shift
  start_step "${step}"
  "$@"
  finish_step
}

require_command() {
  local command_name="$1"
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "This script requires '${command_name}', but it is not installed." >&2
    return 1
  fi
}

require_cargo_deny() {
  require_command cargo
  if ! cargo deny --version >/dev/null 2>&1; then
    echo "This script requires 'cargo-deny' for dependency license checks." >&2
    echo "Install it with: cargo install --locked cargo-deny@${EXPECTED_CARGO_DENY_VERSION}" >&2
    return 1
  fi

  # The TSV output is sensitive to the cargo-deny version: different versions
  # resolve the dependency graph differently, producing diffs that CI rejects.
  # Assert the active binary matches the version CI pins so a shadowed or
  # mismatched install fails loudly instead of generating wrong files.
  local actual_version
  actual_version="$(cargo deny --version | awk '{print $2}')"
  if [ "${actual_version}" != "${EXPECTED_CARGO_DENY_VERSION}" ]; then
    echo "ERROR: cargo-deny version mismatch." >&2
    echo "  expected: ${EXPECTED_CARGO_DENY_VERSION} (pinned by CI)" >&2
    echo "  found:    ${actual_version} ($(command -v cargo-deny))" >&2
    echo "" >&2
    echo "Install the pinned version with:" >&2
    echo "  cargo install --locked cargo-deny@${EXPECTED_CARGO_DENY_VERSION}" >&2
    echo "" >&2
    echo "If multiple cargo-deny binaries are installed, 'which -a cargo-deny'" >&2
    echo "shows which one is shadowing the others on your PATH." >&2
    return 1
  fi
}

require_cargo_about() {
  require_command cargo
  if ! cargo about --version >/dev/null 2>&1; then
    echo "This script requires 'cargo-about' to bundle dependency license texts." >&2
    echo "Install it with: cargo install --locked cargo-about@${EXPECTED_CARGO_ABOUT_VERSION}" >&2
    return 1
  fi

  # As with cargo-deny, the rendered output can shift between versions, so pin
  # the binary to the version CI uses instead of producing a file CI rejects.
  local actual_version
  actual_version="$(cargo about --version | awk '{print $2}')"
  if [ "${actual_version}" != "${EXPECTED_CARGO_ABOUT_VERSION}" ]; then
    echo "ERROR: cargo-about version mismatch." >&2
    echo "  expected: ${EXPECTED_CARGO_ABOUT_VERSION} (pinned by CI)" >&2
    echo "  found:    ${actual_version} ($(command -v cargo-about))" >&2
    echo "" >&2
    echo "Install the pinned version with:" >&2
    echo "  cargo install --locked cargo-about@${EXPECTED_CARGO_ABOUT_VERSION}" >&2
    return 1
  fi
}

validate_args() {
  if [ "$#" -lt 1 ]; then
    usage
    return 1
  fi

  case "$1" in
    check | generate | generate-third-party-licenses)
      if [ "$#" -ne 1 ]; then
        usage
        return 1
      fi
      ;;
    stage-third-party-licenses)
      # The report name selects which wheel's generated files to stage.
      if [ "$#" -ne 2 ]; then
        usage
        return 1
      fi
      ;;
    *)
      usage
      return 1
      ;;
  esac
}

# cat a file, guaranteeing exactly one trailing newline, so a section appended
# after it cannot run onto its last line. Several crates ship a NOTICE with no
# trailing newline. `$(tail -c 1)` strips a trailing newline, so an empty result
# means the file already ends with one.
cat_terminated() {
  cat "$1"
  if [ -n "$(tail -c 1 "$1")" ]; then
    echo ""
  fi
}

# Packages that ship a compiled artifact declare an about.toml. This is found by
# globbing rather than from cargo metadata on purpose: staging runs on every
# wheel-building runner, including Windows, where invoking cargo just to copy a
# few files would be slow and needless.
discover_about_dirs() {
  ABOUT_DIRS="$(find "${REPO_ROOT}" \
    -maxdepth 3 \
    -name target -prune -o \
    -name about.toml -print |
    sed 's|/about.toml$||' |
    sort)"
  if [ -z "${ABOUT_DIRS}" ]; then
    echo "No package declares an about.toml, so there is nothing to do." >&2
    return 1
  fi
}

discover_cargo_dirs() {
  require_command cargo
  require_command jq
  CARGO_DIRS="$(cargo metadata \
    --format-version=1 \
    --no-deps \
    --manifest-path "${REPO_ROOT}/Cargo.toml" |
    jq -r '
      .workspace_members as $workspace_members
      | .packages[]
      | select(.id as $id | $workspace_members | index($id))
      | .manifest_path
      | sub("/Cargo.toml$"; "")
    ' |
    sort)"
}

check_deps_for_dir() {
  local cargo_dir="$1"
  require_cargo_deny
  (
    trap - ERR
    cd "${cargo_dir}"
    cargo deny check license
  )
}

generate_deps_for_dir() {
  local cargo_dir="$1"
  require_cargo_deny
  (
    trap - ERR
    cd "${cargo_dir}"
    cargo deny list -f tsv -t 0.6 > DEPENDENCIES.rust.tsv
  )
}

# Distributing a compiled artifact redistributes every statically linked
# dependency, which triggers the attribution conditions of the permissive
# licenses those dependencies use. Those conditions want the license text, not a
# list of SPDX identifiers, so packages that ship binaries (currently
# bindings/python) also carry an about.toml plus about.hbs and get full texts.
#
# One set of files is written per published wheel, because the resolved crate
# graph differs by target: a Linux wheel links crates a Windows wheel does not.
# Generating per target keeps each wheel's LICENSE, NOTICE, and license bundle an
# exact description of that wheel, which is what ASF policy requires of them.
# deny.toml decides which licenses we may depend on; about.toml's `accepted` decides
# which one we take when a crate offers a choice, and so which text ships. The second
# has to cover everything the first permits, or cargo-about fails on a crate
# cargo-deny was happy with -- a confusing failure that surfaces only when a wheel is
# being built. Checking it here keeps the two files from drifting apart silently.
#
# Both arrays are extracted with sed rather than a TOML parser: this script has to run
# on a stock macOS shell with no Python or extra tooling, and the arrays are simple
# lists of quoted SPDX ids. Per-crate `exceptions` in deny.toml are deliberately not
# compared; cargo-about cannot express them, which about.toml documents.
require_accepted_covers_deny_allow() {
  local cargo_dir="$1"
  local deny_toml="${REPO_ROOT}/deny.toml"
  local about_toml="${cargo_dir}/about.toml"

  [ -f "${deny_toml}" ] || return 0

  local work
  work="$(mktemp -d)"
  # shellcheck disable=SC2064 # Expand work now; it is gone by the time the trap runs.
  trap "rm -rf '${work}'" RETURN

  sed -n '/^allow = \[/,/^\]/p' "${deny_toml}" |
    sed -n 's|^[[:space:]]*"\([^"]*\)".*|\1|p' | sort -u > "${work}/allow"
  sed -n '/^accepted = \[/,/^\]/p' "${about_toml}" |
    sed -n 's|^[[:space:]]*"\([^"]*\)".*|\1|p' | sort -u > "${work}/accepted"

  if [ ! -s "${work}/allow" ] || [ ! -s "${work}/accepted" ]; then
    echo "Could not read the license lists out of ${deny_toml} or ${about_toml}." >&2
    echo "If either file's formatting changed, update require_accepted_covers_deny_allow." >&2
    return 1
  fi

  local missing
  missing="$(comm -23 "${work}/allow" "${work}/accepted")"
  if [ -n "${missing}" ]; then
    echo "${about_toml} is missing licenses that ${deny_toml} allows:" >&2
    echo "${missing}" | sed 's|^|  |' >&2
    echo "" >&2
    echo "cargo-deny permits crates under these, so a dependency could adopt one and" >&2
    echo "break the wheel build. Add them to 'accepted', minding that its order sets" >&2
    echo "which license is chosen for multi-licensed crates." >&2
    return 1
  fi
}

generate_reports_for_dir() {
  local cargo_dir="$1"
  require_cargo_about
  require_command jq
  require_accepted_covers_deny_allow "${cargo_dir}"

  local report_name triples
  while IFS=$'\t' read -r report_name triples; do
    [ -n "${report_name}" ] || continue
    echo "    ${report_name} (${triples})"
    generate_report "${cargo_dir}" "${report_name}" "${triples}"
  done <<< "${WHEEL_LICENSE_REPORTS}"
}

# Write the three legal files describing one wheel into
# ${LICENSES_DIR_NAME}/<report>/. Nothing is staged for packaging here: these are
# reference copies, and stage_reports_for_dir puts one set into place.
generate_report() {
  local cargo_dir="$1"
  local report_name="$2"
  local triples="$3"
  local out_dir="${cargo_dir}/${LICENSES_DIR_NAME}/${report_name}"

  # One --target per triple. cargo-about unions the graphs of repeated flags,
  # which is what a fat binary such as universal2 actually contains. Filtering
  # needs no cross toolchain: only cfg evaluation depends on the triple.
  local target_flags="" triple
  for triple in ${triples}; do
    target_flags="${target_flags} --target ${triple}"
  done

  mkdir -p "${out_dir}"
  generate_license_texts "${cargo_dir}" "${out_dir}" "${report_name}" "${triples}" "${target_flags}"
  generate_notice "${cargo_dir}" "${out_dir}" "${report_name}" "${target_flags}"
  generate_license "${out_dir}" "${report_name}" "${triples}"
}

# `--fail` exits non-zero when a crate's license cannot be reasonably determined,
# which is a narrower condition than it sounds. Crates that vendor foreign C
# sources still declare a license in Cargo.toml, so theirs *is* determined:
# liblzma-sys says "MIT OR Apache-2.0" and zstd-sys says "MIT/Apache-2.0". But
# cargo-about also discovers the license files inside the vendored tree, and one it
# cannot parse is logged, skipped, and does not affect the exit code. The crate
# keeps its declared license and the vendored code silently loses its attribution.
#
# Verified rather than assumed: with the [liblzma-sys.clarify] block removed,
# `cargo about generate --fail` exits 0, logs "failed to parse license 'GPL-2.0'"
# for xz/COPYING.GPLv2, and emits a bundle with no XZ Utils text in it.
#
# So check the log too. The fix for a hit is a `clarify` block in about.toml naming
# the file that applies to the code actually compiled in, not a wider `accepted`
# list -- the license was never rejected, it was never read.
require_all_licenses_parsed() {
  local stderr_file="$1"
  local cargo_dir="$2"

  grep -q 'failed to parse license' "${stderr_file}" || return 0

  echo "cargo-about could not parse a license file and skipped it, so the" >&2
  echo "generated bundle is missing at least one required attribution:" >&2
  grep 'failed to parse license' "${stderr_file}" | sort -u | sed 's|^|  |' >&2
  echo "" >&2
  echo "Add a [<crate>.clarify] block to ${cargo_dir}/about.toml naming the" >&2
  echo "license file that applies to the code compiled into the wheel, as the" >&2
  echo "existing [zstd-sys.clarify] block does." >&2
  return 1
}

# Render the license-text bundle, then substitute the target list into its header.
# cargo-about cannot pass a variable through to the template, so about.hbs carries
# placeholder tokens that are replaced here.
generate_license_texts() {
  local cargo_dir="$1"
  local out_dir="$2"
  local report_name="$3"
  local triples="$4"
  local target_flags="$5"
  local out="${out_dir}/${THIRD_PARTY_LICENSES_FILE}"

  local work
  work="$(mktemp -d)"
  # shellcheck disable=SC2064 # Expand work now; it is gone by the time the trap runs.
  trap "rm -rf '${work}'" RETURN

  # stderr is captured so require_all_licenses_parsed can read it, but it has to be
  # replayed either way, and the exit status collected rather than left to `set -e`:
  # aborting here would kill the shell before anything printed the reason, turning a
  # clear error such as a malformed about.toml into a bare "FAILED".
  local status=0
  (
    trap - ERR
    cd "${cargo_dir}"
    # shellcheck disable=SC2086 # Deliberate word splitting: one flag per triple.
    cargo about generate \
      --frozen \
      --fail \
      --config about.toml \
      ${target_flags} \
      --output-file "${work}/rendered" \
      about.hbs
  ) 2> "${work}/stderr" || status=$?
  cat "${work}/stderr" >&2
  if [ "${status}" -ne 0 ]; then
    echo "cargo-about failed to render ${report_name}; see the error above." >&2
    return "${status}"
  fi
  require_all_licenses_parsed "${work}/stderr" "${cargo_dir}"

  sed \
    -e "s|@@REPORT_NAME@@|${report_name}|g" \
    -e "s|@@TARGET_TRIPLES@@|$(echo "${triples}" | sed 's| |, |g')|g" \
    "${work}/rendered" > "${out}"

  # A renamed or misspelled token would otherwise ship to users verbatim.
  if grep -q -e '@@REPORT_NAME@@' -e '@@TARGET_TRIPLES@@' "${out}"; then
    echo "A placeholder token survived substitution in ${out}." >&2
    echo "Check the token names in ${cargo_dir}/about.hbs." >&2
    return 1
  fi
}

# Apache-2.0 section 4(d) requires a derivative work to relay the attribution
# notices of the Apache-2.0 dependencies it redistributes. Those notices belong in
# NOTICE and not alongside the license texts: the ASF is explicit that NOTICE
# carries "a certain subset of legally required notifications" and that nothing
# else may be added to it, because every addition binds downstream consumers too.
#
# The repository NOTICE is read as the base and never written to. It governs the
# source releases and the crates.io publications, which contain none of this
# third-party code and must not claim otherwise.
generate_notice() {
  local cargo_dir="$1"
  local out_dir="$2"
  local report_name="$3"
  local target_flags="$4"
  local out="${out_dir}/NOTICE"
  local base="${REPO_ROOT}/NOTICE"

  if [ ! -f "${base}" ]; then
    echo "Expected the project NOTICE at ${base}, but it is missing." >&2
    return 1
  fi
  # sha256sum on GNU userlands, shasum on macOS.
  local sha256_cmd
  if command -v sha256sum >/dev/null 2>&1; then
    sha256_cmd="sha256sum"
  elif command -v shasum >/dev/null 2>&1; then
    sha256_cmd="shasum -a 256"
  else
    echo "This script requires 'sha256sum' or 'shasum' to group identical notices." >&2
    return 1
  fi

  local work
  work="$(mktemp -d)"
  # shellcheck disable=SC2064 # Expand work now; it is gone by the time the trap runs.
  trap "rm -rf '${work}'" RETURN

  (
    trap - ERR
    cd "${cargo_dir}"
    # shellcheck disable=SC2086 # Deliberate word splitting: one flag per triple.
    cargo about generate --frozen --fail --config about.toml ${target_flags} --format json
  ) > "${work}/about.json"

  # Sorting by crate name makes both the grouping and the emitted order stable.
  # `source == null` marks a path dependency, i.e. a crate from this workspace.
  # Those are covered by the wheel's own NOTICE and must not be repeated here.
  jq -r '
    [ .crates[].package
      | select(.source != null)
      | {name, version, dir: (.manifest_path | sub("/Cargo.toml$"; ""))}
    ]
    | sort_by(.name, .version)
    | .[]
    | [.name, .version, .dir]
    | @tsv
  ' "${work}/about.json" > "${work}/crates.tsv"

  # bash 3.2 has no associative arrays, so notices are grouped by writing files
  # named after the digest of their contents.
  local name version dir notice digest
  while IFS=$'\t' read -r name version dir; do
    [ -n "${dir}" ] || continue
    for notice in "${dir}"/NOTICE "${dir}"/NOTICE.* "${dir}"/NOTICE-*; do
      [ -f "${notice}" ] || continue
      digest="$(${sha256_cmd} < "${notice}" | awk '{print $1}')"
      if [ ! -f "${work}/${digest}.text" ]; then
        cp "${notice}" "${work}/${digest}.text"
        echo "${digest}" >> "${work}/order"
      fi
      echo "  * ${name} ${version}" >> "${work}/${digest}.crates"
    done
  done < "${work}/crates.tsv"

  # Build the file in scratch and move it into place, so a failure part-way
  # through cannot leave a half-written NOTICE to be packaged.
  {
    cat_terminated "${base}"
    echo ""
    echo "${NOTICE_RELAY_HEADING}"
    echo "-------------------------------------"
    echo ""
    echo "This section applies to the binary wheel built for"
    echo "${report_name}, which statically links the Rust"
    echo "dependencies below into its compiled extension module. It does not apply"
    echo "to source distributions, which contain none of this code."
    echo ""
    if [ -f "${work}/order" ]; then
      echo "Those dependencies ship a NOTICE file of their own. Apache License 2.0"
      echo "section 4(d) requires a derivative work to carry those notices, so they"
      echo "are reproduced here."
      while IFS= read -r digest; do
        echo ""
        echo "================================================================================"
        echo "Applies to:"
        sort -u "${work}/${digest}.crates"
        echo "--------------------------------------------------------------------------------"
        echo ""
        cat_terminated "${work}/${digest}.text"
      done < "${work}/order"
    else
      # Emitted rather than omitted so an empty result is visibly a finding about
      # the dependency graph, not a generator that failed to run.
      echo "None of those dependencies ships a NOTICE file, so there is nothing to"
      echo "relay. Their license texts are in ${THIRD_PARTY_LICENSES_FILE}."
    fi
  } > "${work}/NOTICE.new"

  mv "${work}/NOTICE.new" "${out}"
}

# ASF policy puts license texts in LICENSE, or in a file LICENSE points to. The
# bundle is far too large to inline, so LICENSE gains a pointer to it. This also
# records which wheel the file belongs to, so a set of files staged into the wrong
# wheel is visible to a reader rather than only to CI.
generate_license() {
  local out_dir="$1"
  local report_name="$2"
  local triples="$3"
  local out="${out_dir}/LICENSE"
  local base="${REPO_ROOT}/LICENSE"

  if [ ! -f "${base}" ]; then
    echo "Expected the project LICENSE at ${base}, but it is missing." >&2
    return 1
  fi

  {
    cat_terminated "${base}"
    echo ""
    echo "==============================================================================="
    echo "${LICENSE_APPENDIX_HEADING}"
    echo "==============================================================================="
    echo ""
    echo "This binary wheel bundles a compiled extension module built for"
    echo "${report_name}, linking the Rust target triple(s)"
    echo "$(echo "${triples}" | sed 's| |, |g'). That module statically links"
    echo "third-party Rust crates, so the wheel redistributes them."
    echo ""
    echo "The complete license text of every such crate is in the file"
    echo "${THIRD_PARTY_LICENSES_FILE}, distributed alongside this LICENSE. The"
    echo "attribution notices those crates require are relayed in NOTICE."
    echo ""
    echo "Source distributions contain none of that code and carry neither file."
  } > "${out}"
}

# Put one wheel's generated files where maturin will package them. LICENSE and
# NOTICE are checked-in symlinks to the repository files in a clean tree, so they
# are removed first: copying over the link would rewrite the repository files
# that govern the source releases.
stage_reports_for_dir() {
  local cargo_dir="$1"
  local report_name="$2"
  local src="${cargo_dir}/${LICENSES_DIR_NAME}/${report_name}"

  if [ ! -d "${src}" ]; then
    echo "No generated license report for '${report_name}' at ${src}." >&2
    echo "Reports available:" >&2
    ls "${cargo_dir}/${LICENSES_DIR_NAME}" 2>/dev/null | sed 's|^|  |' >&2 ||
      echo "  (none; run '$0 generate-third-party-licenses' first)" >&2
    return 1
  fi

  local file
  for file in LICENSE NOTICE "${THIRD_PARTY_LICENSES_FILE}"; do
    if [ ! -f "${src}/${file}" ]; then
      echo "Report '${report_name}' is missing ${file}; regenerate it." >&2
      return 1
    fi
    rm -f "${cargo_dir}/${file}"
    cp "${src}/${file}" "${cargo_dir}/${file}"
  done

  echo "NOTE: staged ${report_name} legal files into ${cargo_dir}." >&2
  echo "      LICENSE and NOTICE are checked-in symlinks; restore them with:" >&2
  echo "        git checkout -- ${cargo_dir}/LICENSE ${cargo_dir}/NOTICE" >&2
}

check_deps() {
  run_step "Check dependency licenses in ${REPO_ROOT}" check_deps_for_dir "${REPO_ROOT}"
}

generate_deps() {
  run_step "Discover Cargo workspace package directories" discover_cargo_dirs
  while IFS= read -r cargo_dir; do
    [ -n "${cargo_dir}" ] || continue
    run_step "Generate dependency list in ${cargo_dir}" generate_deps_for_dir "${cargo_dir}"
  done <<< "${CARGO_DIRS}"
}

generate_third_party_licenses() {
  run_step "Discover packages that bundle dependency licenses" discover_about_dirs
  while IFS= read -r cargo_dir; do
    [ -n "${cargo_dir}" ] || continue
    run_step "Generate per-wheel legal files in ${cargo_dir}/${LICENSES_DIR_NAME}" \
      generate_reports_for_dir "${cargo_dir}"
  done <<< "${ABOUT_DIRS}"
}

stage_third_party_licenses() {
  local report_name="$1"

  # Fail on an unknown name rather than on the missing directory, so a typo or a
  # workflow matrix that has drifted from WHEEL_LICENSE_REPORTS says so plainly.
  if ! echo "${WHEEL_LICENSE_REPORTS}" | cut -f1 | grep -q -x -F "${report_name}"; then
    echo "Unknown license report '${report_name}'." >&2
    echo "Known reports:" >&2
    echo "${WHEEL_LICENSE_REPORTS}" | cut -f1 | sed 's|^|  |' >&2
    return 1
  fi

  run_step "Discover packages that bundle dependency licenses" discover_about_dirs
  while IFS= read -r cargo_dir; do
    [ -n "${cargo_dir}" ] || continue
    run_step "Stage ${report_name} legal files into ${cargo_dir}" \
      stage_reports_for_dir "${cargo_dir}" "${report_name}"
  done <<< "${ABOUT_DIRS}"
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
COMMAND="${1:-}"
REPORT_NAME="${2:-}"
CARGO_DIRS=""
ABOUT_DIRS=""

show_help_if_requested "$@"
run_step "Validate dependency command arguments" validate_args "$@"

case "${COMMAND}" in
  check)
    check_deps
    ;;
  generate)
    generate_deps
    ;;
  generate-third-party-licenses)
    generate_third_party_licenses
    ;;
  stage-third-party-licenses)
    stage_third_party_licenses "${REPORT_NAME}"
    ;;
esac
