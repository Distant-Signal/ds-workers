#!/usr/bin/env python3
# ruff: noqa: T201  # a CLI whose findings (or, with --print-tag, the tag) are stdout
"""Check that this repository depends on ds-core at one tag, and on nothing else.

  python3 scripts/check-core-pin.py              # exit 1 on any finding
  python3 scripts/check-core-pin.py --print-tag  # print the ds-core tag

Repo split spec §3.2 and §5.2: a service repository's only first-party
dependency is ds-core, as git dependencies on the public repository, every
core crate on the same tag. This fails when:

  - a [workspace.dependencies] entry is a git dependency on anything but
    CORE_URL, or a CORE_URL dependency without a `tag` (a branch or rev);
  - the CORE_URL entries name more than one tag;
  - Cargo.lock's CORE_URL packages disagree with that tag, or come from more
    than one commit;
  - a member's dependency is a git dependency, or a path outside this
    repository (a sibling service repository, or a local ds-core checkout
    committed by mistake);
  - the root Cargo.toml has a [patch] table (local development patches go
    on the command line, `cargo --config 'patch...'`, never in a commit).

Stdlib only.
"""

import argparse
import re
import sys
import tomllib
from collections.abc import Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
CORE_URL = "https://github.com/Distant-Signal/ds-core"
TAG = re.compile(r"[?&]tag=([^&#]+)")
DEP_TABLES = ("dependencies", "dev-dependencies", "build-dependencies")

Table = Mapping[str, Any]


def dependency_tables(manifest: Table) -> Iterator[tuple[str, Table]]:
    """Yield (table name, table) for every dependency table in a manifest."""
    for key in DEP_TABLES:
        if key in manifest:
            yield key, manifest[key]
    for target, table in manifest.get("target", {}).items():
        for key in DEP_TABLES:
            if key in table:
                yield f"target.{target}.{key}", table[key]


def workspace_problems(manifest: Table) -> tuple[list[str], set[str]]:
    """Return the root manifest's findings and the ds-core tags it names."""
    problems: list[str] = []
    if "patch" in manifest:
        problems.append("Cargo.toml has a [patch] table; patch on the command line")
    tags: set[str] = set()
    deps = manifest.get("workspace", {}).get("dependencies", {})
    for name, spec in deps.items():
        if not isinstance(spec, dict) or "git" not in spec:
            continue
        if spec["git"] != CORE_URL:
            problems.append(f"workspace dependency {name} is on git {spec['git']}")
        elif "tag" not in spec:
            problems.append(f"workspace dependency {name} is on ds-core without a tag")
        else:
            tags.add(spec["tag"])
    if len(tags) > 1:
        problems.append(f"the ds-core crates are on more than one tag: {sorted(tags)}")
    return problems, tags


def member_problems(root: Path, members: Sequence[str]) -> list[str]:
    """Return the members' out-of-tree path and git dependency findings."""
    problems: list[str] = []
    for member in members:
        member_dir = (root / member).resolve()
        crate = tomllib.loads((member_dir / "Cargo.toml").read_text("utf-8"))
        for table_name, table in dependency_tables(crate):
            for name, spec in table.items():
                if not isinstance(spec, dict):
                    continue
                where = f"{member}: {table_name}.{name}"
                if "git" in spec:
                    problems.append(f"{where} is a git dependency; use the workspace's")
                path = spec.get("path")
                if path and not (member_dir / path).resolve().is_relative_to(root):
                    problems.append(f"{where} is a path outside this repository")
    return problems


def lock_problems(lock: Table, tag: str | None) -> list[str]:
    """Return Cargo.lock's findings for the git packages."""
    problems: list[str] = []
    commits: set[str] = set()
    for package in lock.get("package", []):
        source = package.get("source", "")
        if not source.startswith("git+"):
            continue
        url = source.removeprefix("git+").split("?")[0].split("#")[0]
        if url != CORE_URL:
            problems.append(f"Cargo.lock: {package['name']} comes from git {url}")
            continue
        match = TAG.search(source)
        if match is None or (tag is not None and match.group(1) != tag):
            problems.append(f"Cargo.lock: {package['name']} is not on {tag}: {source}")
        commits.add(source.rsplit("#", 1)[-1])
    if len(commits) > 1:
        problems.append(f"Cargo.lock: ds-core comes from {len(commits)} commits")
    return problems


def check(root: Path) -> tuple[list[str], str | None]:
    """Return the findings and the one ds-core tag (None if there isn't one)."""
    root = root.resolve()
    manifest = tomllib.loads((root / "Cargo.toml").read_text("utf-8"))
    problems, tags = workspace_problems(manifest)
    tag = next(iter(tags)) if len(tags) == 1 else None
    members = manifest.get("workspace", {}).get("members", [])
    problems += member_problems(root, members)
    lock = tomllib.loads((root / "Cargo.lock").read_text("utf-8"))
    problems += lock_problems(lock, tag)
    if not tags and not problems:
        problems.append("no ds-core dependency in [workspace.dependencies]")
    return problems, tag


def main(argv: Sequence[str] | None = None) -> int:
    """Run the check."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--print-tag", action="store_true", help="print the ds-core tag"
    )
    parser.add_argument("--root", type=Path, default=ROOT, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    problems, tag = check(args.root)
    for problem in problems:
        print(f"::error::{problem}")
    if problems:
        return 1
    if args.print_tag:
        print(tag)
    return 0


if __name__ == "__main__":
    sys.exit(main())
