"""Every host path the DAGs need is mounted, and every relative default is overridden.

This file exists because of a failure that is invisible from inside the repo: **Airflow tasks
run with `cwd=/opt/airflow`**, so every relative path in `config.py` resolves somewhere that
does not exist, and every file not listed under `volumes:` is simply absent.

`docker-compose.yml` already overrode `SIGNAL_DATA_ROOT`, `SIGNAL_OUT_ROOT` and
`SIGNAL_CACHE_ROOT` for exactly this reason. It did not override the entity dictionary's path,
and did not mount the directory holding it — so `resolve_dag` could never have found its
dictionary. Nobody noticed for two phases, because that DAG had been paused since it was
written and Phase 3 ran the resolver locally from the repo root, where the relative path works.

Reading the YAML with a regex rather than a parser, deliberately: PyYAML is not a dependency of
this project and adding one to assert on a config file is a poor trade.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from signal_core.config import Settings

REPO = Path(__file__).resolve().parents[1]
COMPOSE = (REPO / "docker-compose.yml").read_text(encoding="utf-8")
MAKEFILE = (REPO / "Makefile").read_text(encoding="utf-8")

# `- ./host/path:/container/path` or `...:/container/path:ro`
_MOUNT = re.compile(r"^\s*-\s+(\./[^:\s]+):(/[^:\s]+)(?::\w+)?\s*$", re.MULTILINE)


def _mounts() -> dict[str, str]:
    return {host: container for host, container in _MOUNT.findall(COMPOSE)}


def test_the_entity_dictionary_is_reachable_from_a_container():
    """`resolve_dag`'s hard dependency. Without the mount it raises `FileNotFoundError` on
    every run — which is what it would have done the first time it was ever unpaused."""
    assert "./warehouse" in _mounts(), "warehouse/ is not mounted; the resolver cannot load"


def test_the_committed_dictionary_actually_exists():
    """The mount is worthless if the snapshot is not in the repo. It is a build artifact of
    `signal dictionary`, committed on purpose so `make eval` reproduces (SPEC §7.2)."""
    assert (REPO / Settings().entity_dictionary_path).exists()


@pytest.mark.parametrize(
    ("variable", "setting"),
    [
        ("SIGNAL_DATA_ROOT", "data_root"),
        ("SIGNAL_OUT_ROOT", "out_root"),
        ("SIGNAL_CACHE_ROOT", "cache_root"),
        ("SIGNAL_ENTITY_DICTIONARY_PATH", "entity_dictionary_path"),
    ],
)
def test_every_relative_path_setting_is_overridden_absolutely(variable: str, setting: str):
    """A relative default is correct for a `uv run` from the repo root and wrong for every
    container. Both halves are asserted: the setting's default really is relative (so this
    test is not pinning something already absolute), and compose really does override it with
    an absolute path."""
    default = getattr(Settings(), setting)
    assert not Path(default).is_absolute(), f"{setting} is no longer relative; update this test"

    match = re.search(rf"^\s*{variable}:\s*(\S+)\s*$", COMPOSE, re.MULTILINE)
    assert match, f"{variable} is not set in docker-compose.yml"
    assert match.group(1).startswith("/"), f"{variable} must be absolute, got {match.group(1)}"


def test_make_clean_protects_every_bind_mounted_path_it_could_delete():
    """The guard's inventory is the part that goes stale. Deleting a bind-mounted directory
    while the containers are up breaks the mount at the inode level, and the first version of
    this guard covered `.cache` alone — then promptly broke `out`, where the 16:00 brief
    writes.

    Scoped to what `clean` actually removes: `./src` and `./airflow/dags` are mounted too and
    are never candidates for deletion."""
    protected = set(re.search(r"^MOUNTED_PATHS := (.+)$", MAKEFILE, re.MULTILINE).group(1).split())
    removable = {"data", "out", ".cache", "build"}

    for host_path in _mounts():
        name = host_path.removeprefix("./")
        if name in removable:
            assert name in protected, f"{name} is bind-mounted and deletable but unprotected"


# `KEY: ${KEY:-default}` — the shape that silently imports the local value.
_SELF_INTERPOLATED = re.compile(r"^\s*(SIGNAL_\w+):\s*\$\{\1:-", re.MULTILINE)

# Settings whose correct value genuinely *differs* between the host and a container, because
# they name a location rather than a resource. A container that inherits the host's answer
# for one of these points at itself.
HOST_SPECIFIC = frozenset(
    {
        "SIGNAL_OLLAMA_URL",
        "SIGNAL_DATA_ROOT",
        "SIGNAL_OUT_ROOT",
        "SIGNAL_CACHE_ROOT",
        "SIGNAL_ENTITY_DICTIONARY_PATH",
    }
)


def test_no_host_specific_setting_is_interpolated_from_the_local_env():
    """Compose expands `${VAR}` from the project `.env` *before* the container starts, so
    `KEY: ${KEY:-default}` silently imports whatever the local shell uses — and for a setting
    that names a location, the local answer is the wrong one inside a container.

    `SIGNAL_OLLAMA_URL=http://localhost:11434` is correct for `uv run signal enrich` and
    resolves to the container itself in Airflow. It overrode the `host.docker.internal`
    default, so every enrichment task would have failed with `OllamaUnavailable` against a URL
    nobody set on purpose.

    Resource names (`SIGNAL_BRONZE_BUCKET`, `AWS_REGION`) are deliberately not covered: those
    mean the same thing on both sides and inheriting them is the point.
    """
    leaked = set(_SELF_INTERPOLATED.findall(COMPOSE)) & HOST_SPECIFIC
    assert not leaked, (
        f"{sorted(leaked)} interpolate the local value into the container. "
        "Hardcode them, or use a differently-named override variable."
    )
