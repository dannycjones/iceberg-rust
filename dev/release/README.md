<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# Release Helpers

These scripts support local Apache Iceberg Rust release candidate creation and verification.

## Create an RC

```shell
dev/release/create_rc.sh 0.9.1 2
```

This creates:

- source archive `dist/apache-iceberg-rust-0.9.1-rc2/apache-iceberg-rust-0.9.1.tar.gz`
- detached signature `.asc`
- SHA-512 checksum `.sha512`
- signed annotated tag `v0.9.1-rc.2`

The license header check runs against the generated source archive, not the live Git worktree. The optional SVN upload runs after local artifact verification and before RC tag creation. The signed RC tag is created as the final release step, then the script prints a draft VOTE email for `dev@iceberg.apache.org`. The script logs every step before it runs and after it succeeds. If a step fails, it prints the failed step and stops.

The script always archives and tags `HEAD`, so check out the exact commit to release before running it.

Common options:

```shell
dev/release/create_rc.sh 0.9.1 2 --create_rc_tag 0 --sign 0
dev/release/create_rc.sh 0.9.1 2 --upload_svn 1
dev/release/create_rc.sh 0.9.1 2 --check_headers 0 --check_deps 0 --check_publish 0
```

Defaults:

- `--dist_dir dist`: artifact output root.
- `--create_rc_tag 1`: create the signed annotated RC tag as the final release step.
- `--check_headers 1`: check Apache license headers against the source archive.
- `--check_deps 1`: run dependency license checks before artifact creation.
- `--check_publish 1`: dry-run publishing every crate to crates.io before artifact creation.
- `--sign 1`: create and verify the detached GPG signature.
- `--upload_svn 0`: upload RC artifacts to the ASF dev dist SVN repository.
- `--svn_dist_url https://dist.apache.org/repos/dist/dev/iceberg`: SVN directory URL where the RC artifact directory will be uploaded.

`--sign 1` or `--create_rc_tag 1` requires a local GPG secret key. See the website's GPG setup guide before creating a real release candidate.

`--check_deps 1` requires `cargo-deny`. Install it with:

```shell
cargo install --locked cargo-deny
```

`--check_publish 1` runs `cargo publish --workspace --dry-run`, which packages and compiles every crate and needs network access to crates.io.
Pass `--check_publish 0` to skip it when offline or when iterating on the script.

## Verify an RC

```shell
dev/release/verify_rc.sh 0.9.1 2
```

By default this downloads from `https://dist.apache.org/repos/dist/dev/iceberg/apache-iceberg-rust-0.9.1-rc2/`, verifies checksum and signature with the local GPG keyring, checks source headers, and runs Rust and Python build/tests.

To verify artifacts already created under local `dist/`:

```shell
dev/release/verify_rc.sh 0.9.1 2 --download 0
```

Common options:

```shell
dev/release/verify_rc.sh 0.9.1 2 --verify_signature 0
dev/release/verify_rc.sh 0.9.1 2 --import_gpg_keys 1
dev/release/verify_rc.sh 0.9.1 2 --build 0 --python 0 --check_headers 0
```

Defaults:

- `--dist_dir dist`: local artifact root used when `--download 0`.
- `--download 1`: download artifacts from ASF dev dist.
- `--verify_signature 1`: verify the `.asc` signature with the local GPG keyring.
- `--import_gpg_keys 0`: download and import Apache Iceberg release keys before signature verification.
- `--check_headers 1`: check Apache license headers against the extracted source archive.
- `--build 1`: build and test the Rust source distribution.
- `--python 1`: build and test pyiceberg-core.
- `--tmp_dir <auto>`: verification sandbox; auto-created and deleted on success when omitted.

## Promote an RC to a Release

After the VOTE passes, convert the approved RC to the official release:

```shell
dev/release/release.sh 0.9.1 2
```

This creates the signed annotated final release tag `v0.9.1` from the RC tag `v0.9.1-rc.2`, then moves the ASF SVN artifacts from `dev/iceberg/apache-iceberg-rust-0.9.1-rc2` to `release/iceberg/apache-iceberg-rust-0.9.1`.

Common options:

```shell
dev/release/release.sh 0.9.1 2 --create_release_tag 0
dev/release/release.sh 0.9.1 2 --move_svn 0
```

Defaults:

- `--create_release_tag 1`: create the signed annotated final release git tag.
- `--move_svn 1`: move the RC artifacts from ASF dev dist to ASF release dist.
- `--tag_ref <rc tag commit>`: git commit-ish to tag as the final release.
- `--dev_dist_url https://dist.apache.org/repos/dist/dev/iceberg`: SVN directory URL containing RC artifact directories.
- `--release_dist_url https://dist.apache.org/repos/dist/release/iceberg`: SVN directory URL where final release artifact directories are published.

The script does not push the final release tag. Push it manually after reviewing the output:

```shell
git push origin "v0.9.1"
```

## Dependencies

Use the dependency helper to update or verify dependency license lists:

```shell
dev/release/dependencies.sh generate
dev/release/dependencies.sh check
```

`generate` writes each package's checked-in `DEPENDENCIES.rust.tsv`, which lists
SPDX identifiers only. Packages that ship a compiled artifact also need the full
license texts of the crates linked into it; those carry an `about.toml` (currently
just `bindings/python`) and get a generated set of legal files:

```shell
dev/release/dependencies.sh generate-third-party-licenses
```

That writes `bindings/python/licenses/<wheel>/`, one directory per published
wheel, each holding three files:

- `THIRD-PARTY-LICENSES`, the full license text of every crate linked into that
  wheel's extension module.
- `NOTICE`, the project `NOTICE` plus the `NOTICE` files of those crates, which
  Apache License 2.0 section 4(d) requires a derivative work to relay.
- `LICENSE`, the project `LICENSE` plus a pointer to `THIRD-PARTY-LICENSES`.

There is one set per wheel because the resolved crate graph depends on the target
triple: a Windows wheel links crates a Linux wheel does not. Generating per target
keeps each wheel's files an exact description of that wheel, which is what ASF
policy requires of them. The wheel names are listed in `WHEEL_LICENSE_REPORTS` in
`dependencies.sh`, and each wheel job's matrix entry names the one it needs.

Nothing is packaged by generating. A wheel job stages one set into the package
directory just before `maturin` runs:

```shell
dev/release/dependencies.sh stage-third-party-licenses x86_64-unknown-linux-gnu
```

Everything above is a build output rather than checked-in content, and
`bindings/python/licenses/` is gitignored. Staging overwrites
`bindings/python/LICENSE` and `bindings/python/NOTICE`, which are symlinks to the
repository files in a clean tree, so it leaves the worktree dirty. Restore them
with `git checkout -- bindings/python/LICENSE bindings/python/NOTICE` and delete
`bindings/python/THIRD-PARTY-LICENSES`.

Source distributions must carry none of this: they contain none of that code. The
sdist jobs never stage, so they ship the plain symlinked `LICENSE` and `NOTICE`
and no bundle.

The commands above are for producing local copies to inspect.
`dev/check_wheel_licenses.py <dir> [wheel]` asserts that built wheels actually
contain all three files, and that they are the set generated for the named wheel
rather than another platform's.
