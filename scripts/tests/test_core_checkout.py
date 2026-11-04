"""Tests for scripts/core-checkout.py.

uv run python -m unittest discover -s scripts/tests
"""

import importlib.util
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import ModuleType

SCRIPT = Path(__file__).resolve().parent.parent / "core-checkout.py"


def load_script() -> ModuleType:
    """Import the hyphenated script as a module."""
    spec = importlib.util.spec_from_file_location("core_checkout", SCRIPT)
    if spec is None or spec.loader is None:
        raise ImportError(SCRIPT)
    module = importlib.util.module_from_spec(spec)
    sys.modules["core_checkout"] = module
    spec.loader.exec_module(module)
    return module


core = load_script()
SHA = "0123456789abcdef0123456789abcdef01234567"
LOCK = f"""version = 4

[[package]]
name = "ds-store"
version = "0.1.0"
source = "git+https://github.com/Distant-Signal/ds-core?tag=v0.1.0#{SHA}"

[[package]]
name = "serde"
version = "1.0.0"
source = "registry+https://github.com/rust-lang/crates.io-index"
"""


def git(cwd: Path, *args: str) -> str:
    """Run git in `cwd` as a fixed test identity; return its stdout."""
    return subprocess.run(  # noqa: S603  # a fixed git command
        [  # noqa: S607  # git from PATH
            "git",
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "-c",
            "commit.gpgsign=false",
            *args,
        ],
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


class CoreCheckoutTests(unittest.TestCase):
    """The pin from Cargo.lock, and a checkout that must match it."""

    def test_pinned(self) -> None:
        """The tag and commit come from ds-store's git source."""
        self.assertEqual(core.pinned(LOCK), ("v0.1.0", SHA))
        with self.assertRaises(ValueError):
            core.pinned(LOCK.replace("ds-store", "other"))

    def test_checkout_matches_the_pin(self) -> None:
        """A clone of the tag at the pinned commit; a moved tag fails."""
        with tempfile.TemporaryDirectory() as tmp:
            upstream = Path(tmp) / "ds-core"
            upstream.mkdir()
            git(upstream, "init", "-q", "-b", "main")
            (upstream / "f").write_text("x")
            git(upstream, "add", "f")
            git(upstream, "commit", "-q", "-m", "one")
            git(upstream, "tag", "v0.1.0")
            sha = git(upstream, "rev-parse", "HEAD")
            url = upstream.as_uri()
            dest = core.checkout(Path(tmp) / "out", url, "v0.1.0", sha)
            self.assertEqual((dest / "f").read_text(), "x")
            self.assertEqual(core.checkout(dest, url, "v0.1.0", sha), dest)
            with self.assertRaises(ValueError):
                core.checkout(Path(tmp) / "other", url, "v0.1.0", SHA)
            # A partial copy (no .git) is an error, or replaced if asked.
            stale = Path(tmp) / "stale"
            (stale / "crates").mkdir(parents=True)
            with self.assertRaises(FileExistsError):
                core.checkout(stale, url, "v0.1.0", sha)
            dest = core.checkout(stale, url, "v0.1.0", sha, replace=True)
            self.assertEqual((dest / "f").read_text(), "x")
            self.assertFalse((dest / "crates").exists())


if __name__ == "__main__":
    unittest.main()
