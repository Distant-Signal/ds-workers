#!/usr/bin/env python3
# ruff: noqa: T201  # a CLI whose output (the labels, the findings) is stdout
"""The images' version labels, and the chart's release-ordering checks.

  python3 scripts/release-order.py labels [--github-output]
  python3 scripts/release-order.py check --values FILE --source
  python3 scripts/release-order.py check --values FILE --registry
      [--app-version V]

Repo split spec §5.1 and §5.3, phase 3 item 8. Two labels say what an
image was built against:

  org.distant-signal.core-version  every Rust image: the ds-core version
                                   (ds-store and the other core crates). On
                                   ds-migrate it is the core it carries, so
                                   the schema and grants it applies.
  org.distant-signal.api-build     the frontend: the api build whose types
                                   it was built with. The api carries its
                                   own build, so the check can read it from
                                   the pinned digest.

`labels` prints, as JSON, each image's labels (one `key=value` per line,
the shape Distant-Signal/ci's image.yml takes as its `labels` input), keyed
by service: every docker/<service>.Dockerfile and `frontend`. With
--github-output it also writes `labels=<that JSON>` to $GITHUB_OUTPUT, for
containers.yml's matrix. The values come from Cargo.lock: ds-store pulled
from git at a tag (`?tag=vX.Y.Z`, after the split) gives that tag; a
workspace crate (the monorepo) gives `v<its version>`. The api-build is the
`api` crate's, the same way, and only where the lock has that crate (the
monorepo); after the split the frontend's comes from its api pin
(frontend/e2e/api-image.txt, §5.3) instead. The values are static until a
version changes, so an unchanged image keeps its digest (a run number or a
commit here would restart every Deployment on every deploy). In the
monorepo every image builds from one commit, so every image gets the same
core-version and api-build, and the checks pass trivially.

`check` reads the chart's values (JSON, e.g. `yq -o=json`, or YAML when
PyYAML is installed), finds every first-party image (an `image` map with a
`digest` key and a repository ending in /images/<service>) and checks:

  1. expand: every image's core-version is at most the pinned ds-migrate's
     (`migrate.image`), so no service reaches the cluster expecting a
     migration or grant the chart's migrate Job won't apply;
  2. api-build: the frontend's api-build is at most the pinned api's, so
     the chart never ships a frontend that expects an api it doesn't deploy;
  3. contract: a destructive migration whose header says
       -- contract: <what> (code stopped using it in <commit>;
          safe after core vA.B.C)
     (one line) needs every image's core-version to be at least vA.B.C: no
     pinned image may still be built against a core whose code used what the
     migration drops. The migrations are --migrations: those of the
     ds-core release the chart pins (scripts/core-checkout.py's checkout),
     which the pinned ds-migrate carries.

Versions order within a scheme: `vX.Y.Z` by number, and `build.<run>-<sha>`
(§5.4's image tags) by run. Two schemes don't order, which is a finding:
the switch from one to the other has to land everywhere at once.

--source takes each image's labels from `labels` (this checkout), and
ds-migrate's from its pinned tag (a ds-core release, labelled with its own
version): the pull request check. --registry reads them from the
registry, from the pinned image config (digest, else tag, else
--app-version), anonymously as the cluster pulls: the check before the
chart is pushed. A missing label is a
finding either way.

Findings are `::error::` lines; exit 1 on any, 2 on bad input (unreadable
values, a registry that won't answer). Stdlib only (PyYAML for YAML values).
"""

import argparse
import json
import os
import re
import sys
import time
import tomllib
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
CORE = "org.distant-signal.core-version"
API_BUILD = "org.distant-signal.api-build"
# The crate whose version is the core's: every core crate shares one tag
# (§5.2), and every Rust image links ds-store or its dependencies.
CORE_CRATE = "ds-store"
API_CRATE = "api"
MIGRATIONS = Path("crates/ds-store/migrations")
MIGRATE = "ds-migrate"
API = "api"
FRONTEND = "frontend"
# Images that are not Cargo builds, so carry no core-version.
NOT_RUST = frozenset({FRONTEND})
SAFE_AFTER = re.compile(r"safe after core\s+(\S+?)[).;,]*$")
REPOSITORY = re.compile(r"/images/([a-z0-9][a-z0-9-]*)$")
SEMVER = re.compile(r"v(\d+)\.(\d+)\.(\d+)")
BUILD = re.compile(r"build\.(\d+)-[0-9a-f]{7,40}")
GIT_TAG = re.compile(r"[?&]tag=([^&#]+)")

INDEX_TYPES = (
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
)
MANIFEST_TYPES = (
    f"{INDEX_TYPES[0]}, application/vnd.oci.image.manifest.v1+json, "
    f"{INDEX_TYPES[1]}, application/vnd.docker.distribution.manifest.v2+json"
)


class InputError(Exception):
    """Input the check can't read: exit 2."""


@dataclass(frozen=True, order=True)
class Version:
    """A label value that orders: a semver tag or a build tag."""

    scheme: str
    key: tuple[int, ...]
    text: str = field(compare=False)

    def comparable(self, other: "Version") -> bool:
        """Whether the two are in the same scheme."""
        return self.scheme == other.scheme


def parse_version(text: str) -> Version | None:
    """Return TEXT as a Version, or None when it is neither scheme."""
    if match := SEMVER.fullmatch(text):
        return Version("semver", tuple(int(g) for g in match.groups()), text)
    if match := BUILD.fullmatch(text):
        return Version("build", (int(match.group(1)),), text)
    return None


# --- labels ---------------------------------------------------------------


def crate_version(lock: Mapping[str, Any], crate: str) -> str:
    """CRATE's version as a label: its git tag, else v<its version>."""
    packages = [p for p in lock.get("package", []) if p.get("name") == crate]
    if len(packages) != 1:
        msg = f"Cargo.lock has {len(packages)} {crate} packages, expected 1"
        raise InputError(msg)
    package = packages[0]
    source = package.get("source")
    if source is None:
        return f"v{package['version']}"
    if source.startswith("git+") and (tag := GIT_TAG.search(source)):
        return tag.group(1)
    msg = f"Cargo.lock's {crate} comes from {source}, not a git tag or the workspace"
    raise InputError(msg)


def rust_services(root: Path) -> list[str]:
    """Every docker/<service>.Dockerfile's service."""
    return sorted(
        p.name.removesuffix(".Dockerfile") for p in root.glob("docker/*.Dockerfile")
    )


def image_labels(root: Path) -> dict[str, dict[str, str]]:
    """Each service's version labels, from ROOT's Cargo.lock."""
    lock = tomllib.loads((root / "Cargo.lock").read_text("utf-8"))
    core = crate_version(lock, CORE_CRATE)
    labels: dict[str, dict[str, str]] = {s: {CORE: core} for s in rust_services(root)}
    if any(p.get("name") == API_CRATE for p in lock.get("package", [])):
        api_build = crate_version(lock, API_CRATE)
        labels.setdefault(API, {CORE: core})[API_BUILD] = api_build
        labels[FRONTEND] = {API_BUILD: api_build}
    return labels


def as_label_lines(labels: Mapping[str, Mapping[str, str]]) -> dict[str, str]:
    """Return the `key=value` lines image.yml's `labels` input takes, per service."""
    return {
        service: "\n".join(f"{k}={v}" for k, v in sorted(values.items()))
        for service, values in sorted(labels.items())
    }


# --- the pinned images ----------------------------------------------------


@dataclass(frozen=True)
class Pinned:
    """A first-party image the chart pins."""

    path: str
    service: str
    repository: str
    tag: str
    digest: str

    def reference(self, app_version: str) -> str:
        """Return the pulled reference: digest, else tag, else the appVersion."""
        if self.digest:
            return f"{self.repository}@{self.digest}"
        if not (self.tag or app_version):
            msg = f"{self.path} has no digest or tag, and no --app-version was given"
            raise InputError(msg)
        return f"{self.repository}:{self.tag or app_version}"


def pinned_images(values: Mapping[str, Any], path: str = "") -> Iterator[Pinned]:
    """Every first-party `image` map in VALUES, depth first."""
    for key, value in values.items():
        here = f"{path}.{key}"
        if not isinstance(value, Mapping):
            continue
        repository = value.get("repository")
        if key == "image" and isinstance(repository, str) and "digest" in value:
            if match := REPOSITORY.search(repository):
                yield Pinned(
                    path=here,
                    service=match.group(1),
                    repository=repository,
                    tag=str(value.get("tag") or ""),
                    digest=str(value.get("digest") or ""),
                )
            continue
        yield from pinned_images(value, here)


def contract_floors(
    root: Path, migrations: Path
) -> tuple[list[tuple[str, Version]], list[str]]:
    """Return the `safe after core` versions in the migrations' contract headers."""
    floors: list[tuple[str, Version]] = []
    problems: list[str] = []
    for path in sorted(migrations.glob("*.sql")):
        for line in leading_comments(path.read_text("utf-8")):
            if not re.match(r"--\s*contract:", line) or "safe after core" not in line:
                continue
            match = SAFE_AFTER.search(line)
            version = parse_version(match.group(1)) if match else None
            if version is None or version.scheme != "semver":
                shown = path.relative_to(root) if path.is_relative_to(root) else path
                problems.append(
                    f"::error file={shown}::its contract header's "
                    "`safe after core` is not a vX.Y.Z version"
                )
                continue
            floors.append((path.name, version))
    return floors, problems


def leading_comments(sql: str) -> Iterator[str]:
    """Yield the SQL's leading `--` comment lines (blank lines skipped)."""
    for raw in sql.splitlines():
        line = raw.strip()
        if not line:
            continue
        if not line.startswith("--"):
            return
        yield line


# --- the checks -----------------------------------------------------------


class Checker:
    """The ordering rules over one set of pinned images and their labels."""

    def __init__(
        self,
        images: Sequence[Pinned],
        labels_of: Callable[[Pinned], Mapping[str, str]],
    ) -> None:
        """Read every image's labels once."""
        self.images = images
        self.labels = {image.path: labels_of(image) for image in images}
        self.by_service = {image.service: image for image in images}
        self.findings: list[str] = []
        self.cores: dict[str, Version] = {}
        for image in images:
            if image.service not in NOT_RUST:
                core = self.version(image, CORE)
                if core is not None:
                    self.cores[image.path] = core

    def version(self, image: Pinned, key: str) -> Version | None:
        """Return IMAGE's KEY label as a Version; a finding when it isn't one."""
        text = self.labels[image.path].get(key)
        if text is None:
            self.findings.append(
                f"::error::{image.path} ({image.service}) has no {key} label"
            )
            return None
        parsed = parse_version(text)
        if parsed is None:
            self.findings.append(
                f"::error::{image.path} ({image.service}): {key}={text!r} is "
                "neither vX.Y.Z nor build.<run>-<sha>"
            )
        return parsed

    def at_most(self, what: str, low: Version, high: Version, rule: str) -> None:
        """Add a finding unless LOW orders at or before HIGH."""
        if not low.comparable(high):
            self.findings.append(
                f"::error::{what}: {low.text} and {high.text} don't order "
                f"(different version schemes); {rule}"
            )
        elif low > high:
            self.findings.append(
                f"::error::{what}: {low.text} is newer than {high.text}; {rule}"
            )

    def expand(self) -> None:
        """Rule 1: no image's core-version is newer than ds-migrate's."""
        migrate = self.by_service.get(MIGRATE)
        if migrate is None:
            self.findings.append(
                f"::error::the values pin no {MIGRATE} image (migrate.image)"
            )
            return
        core = self.cores.get(migrate.path)
        if core is None:
            return
        rule = (
            f"the pinned {MIGRATE} ({core.text}) must carry the core every "
            "image expects (spec §5.1, expand)"
        )
        for image in self.images:
            mine = self.cores.get(image.path)
            if image is not migrate and mine is not None:
                what = f"{image.path} ({image.service}) core-version"
                self.at_most(what, mine, core, rule)

    def api_build(self) -> None:
        """Rule 2: the frontend's api-build is no newer than the api's."""
        frontend, api = self.by_service.get(FRONTEND), self.by_service.get(API)
        if frontend is None:
            return
        if api is None:
            self.findings.append("::error::the values pin a frontend but no api image")
            return
        wants, has = self.version(frontend, API_BUILD), self.version(api, API_BUILD)
        if wants is not None and has is not None:
            rule = (
                f"the pinned api ({has.text}) must be at least the build the "
                "frontend's types came from (spec §5.3)"
            )
            self.at_most(f"{frontend.path} (frontend) api-build", wants, has, rule)

    def contract(self, migration: str, floor: Version) -> None:
        """Rule 3: every image's core-version is at least MIGRATION's floor."""
        for image in self.images:
            mine = self.cores.get(image.path)
            if mine is None:
                continue
            if not mine.comparable(floor):
                self.findings.append(
                    f"::error::{migration} is safe after core {floor.text}, which "
                    f"doesn't order against {image.path}'s core-version {mine.text}"
                )
            elif mine < floor:
                self.findings.append(
                    f"::error::{migration} is safe after core {floor.text}, but "
                    f"{image.path} ({image.service}) is built against {mine.text}: "
                    "it may still use what the migration drops (spec §5.1, contract)"
                )


def check(
    images: Sequence[Pinned],
    labels_of: Callable[[Pinned], Mapping[str, str]],
    floors: Sequence[tuple[str, Version]],
) -> list[str]:
    """Return every ordering finding, as `::error::` lines."""
    checker = Checker(images, labels_of)
    checker.expand()
    checker.api_build()
    for migration, floor in floors:
        checker.contract(migration, floor)
    return checker.findings


# --- the registry ---------------------------------------------------------


Fetch = Callable[[str, Mapping[str, str]], tuple[bytes, Mapping[str, str]]]


def http_fetch(url: str, headers: Mapping[str, str]) -> tuple[bytes, Mapping[str, str]]:
    """GET URL; return the body and headers. HTTP errors raise."""
    request = urllib.request.Request(url, headers=dict(headers))  # noqa: S310  # https registry URLs
    with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310  # same
        return response.read(), dict(response.headers)


def bearer_token(fetch: Fetch, challenge: str) -> str:
    """Return an anonymous token for a `Bearer realm=...` challenge."""
    params = dict(re.findall(r'(\w+)="([^"]*)"', challenge))
    realm = params.pop("realm", "")
    if not realm.startswith("https://"):
        msg = f"unexpected registry auth challenge: {challenge!r}"
        raise InputError(msg)
    query = "&".join(
        f"{k}={urllib.parse.quote(v, safe=':/')}" for k, v in params.items()
    )
    body, _ = fetch(f"{realm}?{query}", {})
    token = json.loads(body).get("token") or json.loads(body).get("access_token")
    if not isinstance(token, str):
        msg = f"no token from {realm}"
        raise InputError(msg)
    return token


def registry_labels(reference: str, fetch: Fetch = http_fetch) -> dict[str, str]:
    """Return the labels in REFERENCE's image config (linux/amd64 of an index)."""
    host, _, rest = reference.partition("/")
    if "@" in rest:
        name, _, ref = rest.partition("@")
    else:
        name, _, ref = rest.rpartition(":")
    base = f"https://{host}/v2/{name}"
    headers = {"Accept": MANIFEST_TYPES}
    try:
        body, _ = fetch(f"{base}/manifests/{ref}", headers)
    except urllib.error.HTTPError as error:
        if error.code != 401:  # noqa: PLR2004  # Unauthorized: get a token
            raise
        challenge = error.headers.get("WWW-Authenticate", "")
        headers["Authorization"] = f"Bearer {bearer_token(fetch, challenge)}"
        body, _ = fetch(f"{base}/manifests/{ref}", headers)
    manifest = json.loads(body)
    if manifest.get("mediaType") in INDEX_TYPES or "manifests" in manifest:
        chosen = [
            m
            for m in manifest["manifests"]
            if m.get("platform", {}).get("os") == "linux"
            and m.get("platform", {}).get("architecture") == "amd64"
        ]
        if not chosen:
            msg = f"{reference}: no linux/amd64 image in its index"
            raise InputError(msg)
        body, _ = fetch(f"{base}/manifests/{chosen[0]['digest']}", headers)
        manifest = json.loads(body)
    config_headers = {k: v for k, v in headers.items() if k != "Accept"}
    body, _ = fetch(f"{base}/blobs/{manifest['config']['digest']}", config_headers)
    labels = json.loads(body).get("config", {}).get("Labels") or {}
    return {str(k): str(v) for k, v in labels.items()}


def with_retries(reference: str, fetch: Fetch, attempts: int = 3) -> dict[str, str]:
    """registry_labels, retried after a network or server error."""
    for attempt in range(1, attempts + 1):
        try:
            return registry_labels(reference, fetch)
        except (urllib.error.URLError, TimeoutError) as error:
            if isinstance(error, urllib.error.HTTPError) and error.code < 500:  # noqa: PLR2004
                msg = f"{reference}: HTTP {error.code} (is the package public?)"
                raise InputError(msg) from error
            if attempt == attempts:
                msg = f"{reference}: {error}"
                raise InputError(msg) from error
            time.sleep(5 * attempt)
    raise AssertionError  # unreachable


# --- the CLI --------------------------------------------------------------


def read_values(path: Path) -> Mapping[str, Any]:
    """Return the chart values, from JSON or (with PyYAML) YAML."""
    text = path.read_text("utf-8")
    try:
        values = json.loads(text)
    except json.JSONDecodeError:
        try:
            import yaml  # noqa: PLC0415  # optional: only for YAML values
        except ImportError as error:
            msg = f"{path} is not JSON, and PyYAML is not installed to read YAML"
            raise InputError(msg) from error
        values = yaml.safe_load(text)
    if not isinstance(values, Mapping):
        msg = f"{path} is not a map"
        raise InputError(msg)
    return values


def run_check(args: argparse.Namespace, fetch: Fetch = http_fetch) -> int:
    """Run the `check` subcommand."""
    images = list(pinned_images(read_values(args.values)))
    if args.source:
        source = image_labels(args.root)

        def labels_of(image: Pinned) -> Mapping[str, str]:
            # ds-migrate isn't built here: Distant-Signal/ds-core's release
            # vX.Y.Z builds it labelled core-version=vX.Y.Z, so its pinned
            # tag is its label.
            if image.service == MIGRATE and image.service not in source:
                return {CORE: image.tag} if image.tag else {}
            return source.get(image.service, {})
    else:

        def labels_of(image: Pinned) -> Mapping[str, str]:
            return with_retries(image.reference(args.app_version), fetch)

    migrations = args.migrations or args.root / MIGRATIONS
    floors, problems = contract_floors(args.root, migrations)
    findings = problems + check(images, labels_of, floors)
    for finding in findings:
        print(finding)
    if findings:
        return 1
    mode = "this checkout's labels" if args.source else "the registry's labels"
    print(
        f"release ordering holds for {len(images)} pinned image(s) ({mode}; "
        f"{len(floors)} versioned contract migration(s))"
    )
    return 0


def main(argv: Sequence[str] | None = None, fetch: Fetch = http_fetch) -> int:
    """Run a subcommand; see the module docstring."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--root", type=Path, default=ROOT, help=argparse.SUPPRESS)
    sub = parser.add_subparsers(dest="command", required=True)
    labels = sub.add_parser("labels")
    labels.add_argument("--github-output", action="store_true")
    checks = sub.add_parser("check")
    checks.add_argument("--values", type=Path, required=True)
    where = checks.add_mutually_exclusive_group(required=True)
    where.add_argument("--source", action="store_true")
    where.add_argument("--registry", action="store_true")
    checks.add_argument("--app-version", default="")
    checks.add_argument(
        "--migrations",
        type=Path,
        help="the migrations whose contract headers rule 3 reads (default: "
        "crates/ds-store/migrations; with ds-core as a git dependency, the "
        "pinned ds-core checkout's, scripts/core-checkout.py)",
    )
    args = parser.parse_args(argv)
    try:
        if args.command == "labels":
            lines = json.dumps(as_label_lines(image_labels(args.root)), sort_keys=True)
            print(lines)
            if args.github_output:
                with Path(os.environ["GITHUB_OUTPUT"]).open(
                    "a", encoding="utf-8"
                ) as out:
                    out.write(f"labels={lines}\n")
            return 0
        return run_check(args, fetch)
    except InputError as error:
        print(f"::error::{error}")
        return 2


if __name__ == "__main__":
    sys.exit(main())
