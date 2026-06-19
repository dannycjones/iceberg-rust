#!/usr/bin/env python3
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

"""Generate changelog notes by computing the next version from existing tags
and calling the GitHub "Generate release notes" API.

Requires the gh CLI authenticated with a token that has contents:write permission.
"""

import argparse
import json
import re
import subprocess
import sys


def parse_version_tag(tag: str) -> tuple[int, int, int] | None:
    """Parse a release tag like 'v0.9.1' into (major, minor, patch).
    Returns None for RC tags or non-matching tags."""
    match = re.fullmatch(r"v(\d+)\.(\d+)\.(\d+)", tag)
    if match:
        return int(match.group(1)), int(match.group(2)), int(match.group(3))
    return None


def git_tags() -> list[str]:
    result = subprocess.run(
        ["git", "tag", "--list"],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip().splitlines()


def gh_api_generate_notes(
    repo: str, tag_name: str, target_commitish: str, previous_tag_name: str
) -> str:
    result = subprocess.run(
        [
            "gh",
            "api",
            "--method",
            "POST",
            f"/repos/{repo}/releases/generate-notes",
            "-f",
            f"tag_name={tag_name}",
            "-f",
            f"target_commitish={target_commitish}",
            "-f",
            f"previous_tag_name={previous_tag_name}",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        stderr = result.stderr.strip()
        print(f"ERROR: gh api call failed (exit {result.returncode}):\n{stderr}", file=sys.stderr)
        sys.exit(1)
    response = json.loads(result.stdout)
    return response["body"]


def main():
    parser = argparse.ArgumentParser(
        description="Generate changelog notes by computing the next version from existing tags "
        "and calling the GitHub generate-notes API. "
        "Requires gh CLI authenticated with a token that has contents:write permission.",
    )
    parser.add_argument("repo", help="GitHub repository (owner/repo, e.g. apache/iceberg-rust)")
    parser.add_argument("branch", help="Release branch name (e.g. v0.10)")
    args = parser.parse_args()

    branch_match = re.fullmatch(r"v(\d+)\.(\d+)", args.branch)
    if not branch_match:
        parser.error(f"Branch '{args.branch}' does not match expected pattern 'v<major>.<minor>'")

    major = int(branch_match.group(1))
    minor = int(branch_match.group(2))
    print(f"Branch: {args.branch}", file=sys.stderr)
    print(f"Major: {major}, Minor: {minor}", file=sys.stderr)

    all_tags = git_tags()
    minor_releases: list[tuple[int, int, int]] = []
    for tag in all_tags:
        parsed = parse_version_tag(tag)
        if parsed and parsed[0] == major and parsed[1] == minor:
            minor_releases.append(parsed)
    minor_releases.sort()

    if not minor_releases:
        next_version = f"{major}.{minor}.0"

        previous_releases: list[tuple[int, int, int]] = []
        for tag in all_tags:
            parsed = parse_version_tag(tag)
            if parsed and parsed[0] == major and parsed[1] < minor:
                previous_releases.append(parsed)
        previous_releases.sort()

        if not previous_releases:
            parser.error("No previous release tag found to use as comparison base.")

        prev = previous_releases[-1]
        previous_tag = f"v{prev[0]}.{prev[1]}.{prev[2]}"
    else:
        latest = minor_releases[-1]
        next_patch = latest[2] + 1
        next_version = f"{major}.{minor}.{next_patch}"
        previous_tag = f"v{latest[0]}.{latest[1]}.{latest[2]}"

    print(f"Next version: v{next_version}", file=sys.stderr)
    print(f"Previous tag: {previous_tag}", file=sys.stderr)

    notes_body = gh_api_generate_notes(
        repo=args.repo,
        tag_name=f"v{next_version}",
        target_commitish=args.branch,
        previous_tag_name=previous_tag,
    )

    print(f"## Draft Release Notes for v{next_version}")
    print()
    print(f"**Branch:** `{args.branch}`")
    print(f"**Previous tag:** `{previous_tag}`")
    print(f"**Next version tag:** `v{next_version}`")
    print()
    print("---")
    print()
    print("```markdown")
    print(notes_body)
    print("```")


if __name__ == "__main__":
    main()
