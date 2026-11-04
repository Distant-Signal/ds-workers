#!/usr/bin/env python3
# ruff: noqa: T201  # a CLI whose output (the checkout's path) is stdout
"""Check out the ds-core commit Cargo.lock pins, and print its path.

  uv run scripts/core-checkout.py [--dest DIR]

The core crates are git dependencies on Distant-Signal/ds-core, pinned by
tag (repo split spec §5.2). This repository's DB tests still need ds-core's
files at that exact commit (spec §5.1, decided at phase 4): the migrations
(`sqlx migrate run`), and the role scripts and roles setup that
`<checkout>/scripts/test-postgres-roles.py` runs (it reads its own
repository's access/ and migrations). The commit is the one Cargo.lock records for
ds-store (`git+<url>?tag=<tag>#<sha>`); a shallow clone of the tag must be
that commit, or this fails.

The default destination is target/ds-core-<sha> (git-ignored, outside the
images' build context). An existing checkout at the right commit is reused;
anything else there (a partial copy a CI cache restored into target/) is
replaced.
DS_CORE_GIT_URL replaces the clone URL (a local mirror, a rehearsal's
file:// stand-in); the pin is still checked.

Stdlib only.
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
import tomllib
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

ROOT = Path(__file__).resolve().parent.parent
CORE_REPO = "https://github.com/Distant-Signal/ds-core"
SOURCE = re.compile(r"^git\+(?P<url>[^?#]+)\?tag=(?P<tag>[^#]+)#(?P<sha>[0-9a-f]{40})$")


def pinned(lock_text: str, package: str = "ds-store") -> tuple[str, str]:
    """Return (tag, sha) Cargo.lock records for `package` from ds-core."""
    for entry in tomllib.loads(lock_text).get("package", []):
        if entry.get("name") != package:
            continue
        match = SOURCE.match(entry.get("source", ""))
        if match and match.group("url") == CORE_REPO:
            return match.group("tag"), match.group("sha")
    msg = f"Cargo.lock has no {package} from {CORE_REPO} pinned by tag"
    raise ValueError(msg)


def git(*args: str, cwd: Path | None = None) -> str:
    """Return a git command's stdout."""
    return subprocess.run(  # noqa: S603  # git with this script's arguments
        ["git", *args],  # noqa: S607  # git from PATH
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def is_checkout_at(dest: Path, sha: str) -> bool:
    """Whether `dest` is a git checkout whose HEAD is `sha`."""
    if not (dest / ".git").exists():
        return False
    try:
        return git("rev-parse", "HEAD", cwd=dest) == sha
    except subprocess.CalledProcessError:
        return False


def checkout(
    dest: Path, url: str, tag: str, sha: str, *, replace: bool = False
) -> Path:
    """Make `dest` a checkout of `tag` at `sha`; return it.

    Anything else already at `dest` is removed when `replace`, else an error.
    """
    if is_checkout_at(dest, sha):
        return dest
    if dest.exists():
        if not replace:
            msg = f"{dest} exists and isn't ds-core at {sha}: remove it"
            raise FileExistsError(msg)
        shutil.rmtree(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    git("clone", "--quiet", "--depth", "1", "--branch", tag, url, str(dest))
    head = git("rev-parse", "HEAD", cwd=dest)
    if head != sha:
        msg = f"{url} {tag} is {head}, but Cargo.lock pins {sha}: was the tag moved?"
        raise ValueError(msg)
    return dest


def main(argv: Sequence[str] | None = None) -> int:
    """Check out the pinned ds-core and print its path."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dest", type=Path, help="default: target/ds-core-<sha>")
    parser.add_argument("--root", type=Path, default=ROOT, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    tag, sha = pinned((args.root / "Cargo.lock").read_text(encoding="utf-8"))
    dest = args.dest or args.root / "target" / f"ds-core-{sha}"
    url = os.environ.get("DS_CORE_GIT_URL") or CORE_REPO
    # Only the default destination (our own directory under target/) is
    # replaced; a --dest that holds something else is an error.
    print(checkout(dest.resolve(), url, tag, sha, replace=args.dest is None))
    return 0


if __name__ == "__main__":
    sys.exit(main())
