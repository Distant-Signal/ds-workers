#!/usr/bin/env python3
# ruff: noqa: T201  # a CLI: its report is stdout
"""Check that the notifier's SQL only names relations its role is granted.

  python3 scripts/check-notifier-grants.py [--grants db-grants.yaml]

The notifier's DB tests run as its role with `--observe` (table-wide DML and
SELECT on every table, .github/db-tests.toml), so they cannot catch a grant
the role lacks. This check can: it reads every relation a non-test statement
in crates/notifier/src names after FROM, JOIN, INTO or UPDATE, and checks
ds-core's db-grants.yaml (the pinned release's, via scripts/core-checkout.py,
unless --grants names one) gives the `notifier` role a grant on it. A relation
the `read_shared` group used to cover (shared-train, ingest, derived and
reference tables) needs a table-level SELECT of its own once the notifier is
not in that group (ds-core DS-534).

Limits: it sees only SQL written in this crate (not the ds-core functions it
calls: ds_store::trains, journeys, recurrence, ...), and a relation name
only counts after one of those keywords. Test code (`#[cfg(test)]` items at
column 0, whole-file test modules) and comments are skipped.

Stdlib only.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "crates" / "notifier" / "src"
# Files that are test modules as a whole (main.rs: `#[cfg(test)] mod ...;`).
TEST_FILES = frozenset({"settings_db_tests.rs", "webhook_e2e_tests.rs"})
# The classes the read_shared group can SELECT (ds-core's gen-db-grants.py).
GROUP_CLASSES = frozenset({"shared-train", "ingest", "derived", "reference"})
TABLE_LINE = re.compile(r"^  ([a-z_0-9]+): \{class: ([\w-]+)(.*)\}\s*$")
RELATION_USE = re.compile(r"\b(?:FROM|JOIN|INTO|UPDATE)(?:\s|\\)+([a-z_][a-z0-9_]*)")


@dataclass(frozen=True)
class Relation:
    """One table or view of db-grants.yaml, with the notifier's grant."""

    name: str
    cls: str
    # The notifier's grant spec as written (None: no notifier grant).
    notifier: str | None


def _split_top(text: str) -> list[str]:
    """Split `text` on commas outside brackets and braces."""
    parts: list[str] = []
    depth = 0
    start = 0
    for i, char in enumerate(text):
        if char in "[{":
            depth += 1
        elif char in "]}":
            depth -= 1
        elif char == "," and depth == 0:
            parts.append(text[start:i].strip())
            start = i + 1
    parts.append(text[start:].strip())
    return [part for part in parts if part]


def notifier_spec(grants: str) -> str | None:
    """Return the `notifier:` value in a flow `grants` mapping body."""
    for item in _split_top(grants):
        key, _, value = item.partition(":")
        if key.strip() == "notifier":
            return value.strip()
    return None


def has_table_select(spec: str) -> bool:
    """Whether `spec` gives a table-level (not column-limited) SELECT."""
    inner = spec.strip()
    items = _split_top(inner[1:-1]) if inner.startswith("[") else [inner]
    return any(not item.startswith("{") and "S" in item for item in items)


def parse_grants(text: str) -> tuple[dict[str, Relation], bool]:
    """Return (relations, the notifier is in read_shared) from db-grants.yaml."""
    relations: dict[str, Relation] = {}
    section = ""
    role = ""
    in_group = False
    for line in text.splitlines():
        top = re.match(r"^([a-z_]+):", line)
        if top:
            section = top.group(1)
        if section == "roles":
            named = re.match(r"^  ([a-z_]+):\s*$", line)
            if named:
                role = named.group(1)
            groups = re.match(r"^\s+groups: \[(.*)\]", line)
            if groups and role == "notifier":
                in_group = "read_shared" in groups.group(1)
        if section not in {"tables", "views"}:
            continue
        match = TABLE_LINE.match(line)
        if not match:
            continue
        name, cls, rest = match.groups()
        body = re.search(r"grants: \{(.*)\}\s*$", rest)
        spec = notifier_spec(body.group(1)) if body else None
        relations[name] = Relation(name, cls, spec)
    return relations, in_group


def strip_test_code(text: str) -> str:
    """Drop comments and column-0 `#[cfg(test)]` items from Rust source."""
    lines = [line for line in text.splitlines() if not line.lstrip().startswith("//")]
    kept: list[str] = []
    i = 0
    while i < len(lines):
        if lines[i] != "#[cfg(test)]":
            kept.append(lines[i])
            i += 1
            continue
        i += 1
        # The item: skip to its closing line (`}` at column 0, or a `;` line).
        while i < len(lines) and lines[i].startswith("#["):
            i += 1
        opens = i < len(lines) and "{" in lines[i] and ";" not in lines[i]
        while i < len(lines):
            done = lines[i] == "}" if opens else lines[i].rstrip().endswith((";", "}"))
            i += 1
            if done:
                break
    return "\n".join(kept)


def used_relations(
    sources: Iterable[tuple[str, str]], known: Iterable[str]
) -> dict[str, set[str]]:
    """Return {relation: {files}} for the known relations the SQL names."""
    names = set(known)
    used: dict[str, set[str]] = {}
    for filename, text in sources:
        if filename in TEST_FILES:
            continue
        for match in RELATION_USE.finditer(strip_test_code(text)):
            if match.group(1) in names:
                used.setdefault(match.group(1), set()).add(filename)
    return used


def problems(
    used: dict[str, set[str]], relations: dict[str, Relation], *, in_group: bool
) -> list[str]:
    """Return one message per relation the notifier's role cannot read."""
    found: list[str] = []
    for name, files in sorted(used.items()):
        relation = relations[name]
        where = ", ".join(sorted(files))
        covered = in_group and relation.cls in GROUP_CLASSES
        if relation.notifier is None and not covered:
            found.append(f"{name} (used in {where}): no grant for the notifier role")
        elif (
            relation.cls in GROUP_CLASSES
            and not in_group
            and relation.notifier is not None
            and not has_table_select(relation.notifier)
        ):
            found.append(
                f"{name} (used in {where}): read_shared no longer covers it and "
                "the notifier's grant has no table-level SELECT"
            )
    return found


def default_grants() -> Path:
    """Return db-grants.yaml in the ds-core checkout Cargo.lock pins."""
    out = subprocess.run(  # noqa: S603  # fixed argv, this repository's script
        [sys.executable, str(ROOT / "scripts" / "core-checkout.py")],
        check=True,
        capture_output=True,
        text=True,
    )
    return (
        Path(out.stdout.strip().splitlines()[-1])
        / "crates"
        / "ds-store"
        / "db-grants.yaml"
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Run the check; return the exit status."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--grants", type=Path, help="db-grants.yaml (default: the pinned ds-core's)"
    )
    args = parser.parse_args(argv)
    grants: Path = args.grants or default_grants()
    relations, in_group = parse_grants(grants.read_text())
    sources = [(path.name, path.read_text()) for path in sorted(SRC.glob("*.rs"))]
    used = used_relations(sources, relations)
    found = problems(used, relations, in_group=in_group)
    state = "a member of" if in_group else "not in"
    print(
        f"notifier is {state} read_shared; {len(used)} relations named in non-test SQL "
        f"({grants})"
    )
    for message in found:
        print(f"::error::{message}")
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main())
