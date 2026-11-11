#!/usr/bin/env python3
# ruff: noqa: T201  # a CLI: its progress report is stdout
r"""Build the DB/Redis-backed test binaries once; run each CI pass from them.

  scripts/db-tests.py archive --archive-file FILE
  scripts/db-tests.py run PASS [--archive-file FILE] [--dry-run]
  scripts/db-tests.py list-passes

CI's DB tests (the `#[ignore]`d tests that need DATABASE_URL, REDIS_URL or
fake-gcs-server) run several times: as the superuser, as the schema owner
(scripts/test-postgres-roles.py --mode existing / new), and as each
service's own role (--mode per-service, with and without --narrow). With
`cargo test`, each pass's different `-p` selection unified features
differently, so `common`, `ds-store`, `api` and the rest were compiled
again for most passes.

Here they are built once, as a cargo-nextest archive of every package in
ARCHIVE_PACKAGES (one feature resolution: `cargo nextest archive -p ...`),
and each pass runs its steps from that archive with `cargo nextest run
--archive-file`, so a CI job that only runs tests needs no Rust toolchain.
Without --archive-file, `run` builds and runs from the source tree instead
(`cargo nextest run`, the same package selection), for local use.

Each step is one former `cargo test` invocation, with the same libtest
arguments: nextest emulates `--ignored`, `--skip`, `--exact` and name
filters after `--`. `--test-threads=1` becomes `-j 1`. A step with a
`role` runs with DATABASE_URL set to DATABASE_URL_<ROLE>, which
test-postgres-roles.py --mode per-service provides; DS_TEST_NARROWED_ROLES
and the other variables pass through unchanged. Steps run in order and the
pass stops at the first failing step, as the `&&` chains did.

Needs cargo-nextest on PATH (and cargo, for `archive` or a run without
--archive-file). Stdlib only.
"""

import argparse
import os
import shlex
import subprocess
import sys
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Every package a step below tests. The archive builds their test binaries
# with one feature resolution; `ds-runtime`'s and `common`'s `postgres`
# features are named explicitly (pg::tests need ds-runtime's; api and the
# others enable them anyway).
# The core crates (ds-store, ds-migrate, common, ds-runtime, ingest-stream)
# are git dependencies on Distant-Signal/ds-core, which runs their DB
# suites itself (repo split spec §9, phase 4).
ARCHIVE_PACKAGES: tuple[str, ...] = (
    "api",
    "aggregator",
    "notifier",
    "schedule-reference",
    "enricher",
    "movement-feed",
    "ingest-writer",
    "full-coverage-consumer",
    "trust-consumer",
    "schedule-ingest",
    "trust-backlog-consumer",
)
ARCHIVE_FEATURES = ("common/postgres", "ds-runtime/postgres")

# The modules of `#[sqlx::test]` tests: each test creates a database, a
# test-harness right (CREATEDB) the runtime roles do not have. They run as
# the superuser only.
SQLX_TEST_SKIPS = (
    "corpus::db_tests",
    "crosswalk::db_tests",
    "freshness::db_tests",
    "line_timetable::db_tests",
    "auth::sliding_session_tests",
    "reads::db_tests",
    "users::sliding_session_db_tests",
)
# enricher: the LLM-gated live evals and the offline replay harness need
# credentials or an exported history file CI does not have.
ENRICHER_SKIPS = ("live_eval", "replay_")


@dataclass(frozen=True)
class Step:
    """One former `cargo test -p ... -- --ignored ...` invocation."""

    packages: tuple[str, ...]
    # libtest name filters (substring; any one matching selects a test).
    filters: tuple[str, ...] = ()
    skips: tuple[str, ...] = ()
    exact: bool = False
    # `--test NAME` / `--bin NAME`: only that target of the (one) package.
    test_target: str | None = None
    bin_target: str | None = None
    # Run with DATABASE_URL=$DATABASE_URL_<ROLE> (test-postgres-roles.py
    # --mode per-service sets them).
    role: str | None = None
    # --test-threads=1 unless False (fake-gcs-server's tests ran in parallel).
    serial: bool = True
    env: Mapping[str, str] = field(default_factory=dict)

    def filterset(self) -> str:
        """Return the nextest -E expression selecting this step's binaries."""
        packages = " | ".join(f"package(={p})" for p in self.packages)
        if self.test_target is not None or self.bin_target is not None:
            (package,) = self.packages
            binary = (
                f"{package}::{self.test_target}"
                if self.test_target is not None
                else f"{package}::bin/{self.bin_target}"
            )
            return f"binary_id(={binary})"
        return packages if len(self.packages) == 1 else f"({packages})"

    def libtest_args(self) -> list[str]:
        """Return the arguments after `--`, as `cargo test` had them."""
        args = ["--ignored", *self.filters]
        for skip in self.skips:
            args += ["--skip", skip]
        if self.exact:
            args.append("--exact")
        return args

    def describe(self) -> str:
        """Return the `cargo test` command this step replaces."""
        argv = ["cargo", "test"]
        for package in self.packages:
            argv += ["-p", package]
        if self.test_target is not None:
            argv += ["--test", self.test_target]
        if self.bin_target is not None:
            argv += ["--bin", self.bin_target]
        argv += self.filters if not self.exact else ()
        argv += ["--", "--ignored"]
        argv += self.filters if self.exact else ()
        if self.exact:
            argv.append("--exact")
        if self.serial:
            argv.append("--test-threads=1")
        for skip in self.skips:
            argv += ["--skip", skip]
        prefix = (
            f'DATABASE_URL="$DATABASE_URL_{self.role.upper()}" ' if self.role else ""
        )
        return prefix + shlex.join(argv)


def per_service(  # noqa: PLR0913  # Step's own fields
    package: str,
    role: str,
    *,
    filters: tuple[str, ...] = (),
    skips: tuple[str, ...] = (),
    exact: bool = False,
    test_target: str | None = None,
    bin_target: str | None = None,
) -> Step:
    """Return a step running `package`'s suite as `role`."""
    return Step(
        packages=(package,),
        filters=filters,
        skips=skips,
        exact=exact,
        test_target=test_target,
        bin_target=bin_target,
        role=role,
    )


PASSES: Mapping[str, tuple[Step, ...]] = {
    # As the superuser on the migrated `postgres` database. The
    # #[sqlx::test] modules run only here.
    "superuser": (
        Step(
            packages=(
                "api",
                "aggregator",
                "notifier",
                "schedule-reference",
            )
        ),
        Step(packages=("enricher",), skips=ENRICHER_SKIPS),
        Step(packages=("movement-feed",)),
        Step(packages=("ingest-writer",)),
        # The phase 4 readers' db sources (#[sqlx::test]).
        Step(
            packages=("full-coverage-consumer",),
            filters=("population_reload::db_tests",),
        ),
        Step(packages=("trust-consumer",), filters=("reads::",)),
        Step(packages=("schedule-ingest",), filters=("sink::db_tests",)),
        # Against fake-gcs-server (FAKE_GCS_URL; the job starts it).
        Step(
            packages=("schedule-ingest",), filters=("bucket_e2e_tests",), serial=False
        ),
        Step(packages=("trust-backlog-consumer", "trust-consumer")),
    ),
    # test-postgres-roles.py --mode existing: DATABASE_URL is the owner role.
    "existing": (
        Step(
            packages=("api", "aggregator", "notifier"),
            skips=SQLX_TEST_SKIPS,
        ),
        Step(packages=("ingest-writer",)),
        Step(packages=("schedule-reference",)),
        Step(packages=("enricher",), skips=ENRICHER_SKIPS),
    ),
    # test-postgres-roles.py --mode per-service (Stage 0b): each suite as
    # its own service's role. See the CI step's comment for each role.
    "per-service": (
        per_service("api", "api", skips=SQLX_TEST_SKIPS),
        per_service("aggregator", "aggregator", skips=SQLX_TEST_SKIPS),
        per_service("notifier", "notifier", skips=SQLX_TEST_SKIPS),
        per_service("ingest-writer", "writer"),
        per_service("schedule-ingest", "schedule_ingest", filters=("sink::db_tests",)),
        per_service("schedule-reference", "schedule_reference"),
        per_service("enricher", "enricher", skips=ENRICHER_SKIPS),
        per_service("trust-backlog-consumer", "trust_backlog"),
        per_service("trust-consumer", "trust_consumer", skips=("reads::",)),
        per_service(
            "full-coverage-consumer", "full_coverage_ro", test_target="role_reads"
        ),
        per_service("ingest-writer", "incidents", bin_target="writer-maintenance"),
    ),
    # test-postgres-roles.py --mode per-service --narrow
    # api,aggregator,enricher,notifier (release D).
    "narrow": (
        per_service(
            "api",
            "api",
            filters=(
                (
                    "routes::train::db_tests::"
                    "post_track_by_uid_backfills_schedule_and_movement_data_from_the_backlog"
                ),
            ),
            exact=True,
        ),
        per_service("aggregator", "aggregator", filters=("archive::tests::",)),
    ),
}


# --- This repository's share (repo split spec §5.1, "DB tests in other
# repositories": the monorepo's passes, split by crate). ds-core's own
# packages (ds-store, ds-migrate, ds-runtime, common, ingest-stream) are
# tested in ds-core. ---------------------------------------------------------
REPO_PACKAGES = frozenset({"aggregator", "enricher", "notifier"})
ARCHIVE_PACKAGES = tuple(p for p in ARCHIVE_PACKAGES if p in REPO_PACKAGES)
PASSES = {
    name: narrowed
    for name, steps in PASSES.items()
    if (
        narrowed := tuple(
            replace(step, packages=kept)
            for step in steps
            if (kept := tuple(p for p in step.packages if p in REPO_PACKAGES))
        )
    )
}


def reuse_args(extracted: Path, workspace: Path = ROOT) -> list[str]:
    """Return nextest's arguments for running from an extracted archive.

    `--archive-file` would extract the whole archive (about 1 GB) again for
    every step; the binaries and metadata it extracted once are reused.
    """
    target = extracted / "target"
    meta = target / "nextest"
    return [
        "--binaries-metadata",
        str(meta / "binaries-metadata.json"),
        "--cargo-metadata",
        str(meta / "cargo-metadata.json"),
        "--target-dir-remap",
        str(target),
        "--workspace-remap",
        str(workspace),
    ]


def extract_argv(archive: Path, destination: Path) -> list[str]:
    """Return a command extracting `archive` into `destination` (listing nothing)."""
    return [
        "cargo-nextest",
        "nextest",
        "list",
        "--archive-file",
        str(archive),
        "--extract-to",
        str(destination),
        "--extract-overwrite",
        "--workspace-remap",
        str(ROOT),
        "-E",
        "none()",
    ]


def nextest_argv(step: Step, extracted: Path | None) -> list[str]:
    """Return the cargo-nextest command line for one step.

    `extracted`: where the archive was extracted; None builds from source.
    """
    if extracted is not None:
        argv = ["cargo-nextest", "nextest", "run", *reuse_args(extracted)]
    else:
        argv = ["cargo", "nextest", "run", "--locked"]
        for package in ARCHIVE_PACKAGES:
            argv += ["-p", package]
        argv += ["--features", ",".join(ARCHIVE_FEATURES)]
    argv += ["--no-fail-fast", "-E", step.filterset()]
    if step.serial:
        argv += ["--test-threads", "1"]
    return [*argv, "--", *step.libtest_args()]


def archive_argv(archive: Path) -> list[str]:
    """Return the `cargo nextest archive` command line building every package."""
    argv = ["cargo", "nextest", "archive", "--locked"]
    for package in ARCHIVE_PACKAGES:
        argv += ["-p", package]
    return [
        *argv,
        "--features",
        ",".join(ARCHIVE_FEATURES),
        "--archive-file",
        str(archive),
    ]


def step_env(step: Step, base: Mapping[str, str]) -> dict[str, str]:
    """Return the environment for one step; KeyError if its role URL is unset."""
    env = dict(base)
    env.update(step.env)
    if step.role is not None:
        name = f"DATABASE_URL_{step.role.upper()}"
        if not base.get(name):
            raise KeyError(name)
        env["DATABASE_URL"] = base[name]
    return env


def run_pass(
    name: str,
    archive: Path | None,
    *,
    extract_to: Path | None = None,
    dry_run: bool = False,
    base_env: Mapping[str, str] | None = None,
) -> int:
    """Run every step of a pass in order; return the first failing status.

    The archive is extracted once, into `extract_to` (its `target/` lands
    there) or a temporary directory.
    """
    env_base = dict(os.environ if base_env is None else base_env)
    with tempfile.TemporaryDirectory(prefix="db-tests-") as scratch:
        extracted = None
        if archive is not None:
            extracted = Path(scratch) if extract_to is None else extract_to
            argv = extract_argv(archive, extracted)
            print(f"== {shlex.join(argv)}", flush=True)
            if not dry_run:
                status = subprocess.run(argv, check=False, env=env_base).returncode  # noqa: S603  # argv built above
                if status != 0:
                    return status
        for number, step in enumerate(PASSES[name], 1):
            argv = nextest_argv(step, extracted)
            print(f"== {name} step {number}: {step.describe()}", flush=True)
            print(f"== {shlex.join(argv)}", flush=True)
            if dry_run:
                continue
            try:
                env = step_env(step, env_base)
            except KeyError as err:
                print(
                    f"== {err.args[0]} is not set: run this pass under "
                    "test-postgres-roles.py --mode per-service"
                )
                return 2
            status = subprocess.run(argv, check=False, env=env).returncode  # noqa: S603  # argv built above
            if status != 0:
                print(f"== {name} step {number} exited {status}", flush=True)
                return status
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """Parse the command line and dispatch."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    build = sub.add_parser("archive", help="build the test binaries into an archive")
    build.add_argument("--archive-file", type=Path, required=True)
    build.add_argument("--dry-run", action="store_true")
    run = sub.add_parser("run", help="run one pass's steps")
    run.add_argument("pass_name", metavar="PASS", choices=sorted(PASSES))
    run.add_argument("--archive-file", type=Path)
    run.add_argument(
        "--extract-to",
        type=Path,
        help=(
            "extract the archive here (its target/ lands in DIR/target; default: "
            "a temporary directory). CI passes the checkout, where the build job's "
            'target/ was: `env!("CARGO_BIN_EXE_api")` and other build-time paths '
            "are compiled in"
        ),
    )
    run.add_argument("--dry-run", action="store_true", help="print the commands only")
    sub.add_parser("list-passes", help="print each pass's steps as cargo test commands")
    args = parser.parse_args(argv)

    if args.command == "archive":
        command = archive_argv(args.archive_file)
        print(f"== {shlex.join(command)}", flush=True)
        if args.dry_run:
            return 0
        return subprocess.run(command, check=False).returncode  # noqa: S603  # argv built above
    if args.command == "run":
        archive = None if args.archive_file is None else args.archive_file.resolve()
        return run_pass(
            args.pass_name,
            archive,
            extract_to=None if args.extract_to is None else args.extract_to.resolve(),
            dry_run=args.dry_run,
        )
    for name, steps in PASSES.items():
        print(f"{name}:")
        for step in steps:
            print(f"  {step.describe()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
