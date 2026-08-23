"""The enrichment stage end to end. SPEC §7.3; 4B.D.

Athena is faked and Ollama is intercepted with `respx`, so nothing here touches a real
account or a real GPU. What is defended is the governance: never re-infer what the cache
holds, never store what does not validate, never retry forever, and never turn "the server
is off" into a table full of rejects.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
import respx

from signal_core.config import Settings
from signal_core.enrich import store
from signal_core.enrich.run import (
    EnrichmentRun,
    _cache_keys,
    cluster_input,
    enrich_clusters,
    read_for_clusters,
)

NOW = datetime(2026, 8, 22, 6, 0, tzinfo=UTC)
BASE = "http://localhost:11434"
SETTINGS = Settings(
    ollama_url=BASE,
    ollama_model="llama3.1:8b",
    ollama_model_digest="sha256:pinned",
    prompt_version="v1",
)

ANSWER = {
    "summary": "Northwind acquired Lumen Robotics for an undisclosed sum on Tuesday.",
    "topic": "business-corporate",
    "extraction": {"company": "Northwind"},
}

ENRICHMENT_COLUMNS = ["cluster_id", "input_hash", "summary", "topic", "extracted_json"]
ATTEMPT_COLUMNS = ["input_hash", "attempts"]
COLUMN_COLUMNS = ["column_name"]


class _Paginator:
    """Athena's first row is the column header, not data — including on a zero-row result."""

    def __init__(self, columns: list[str], rows: list[list[str | None]]) -> None:
        self._columns, self._rows = columns, rows

    def paginate(self, **_: Any):
        yield {
            "ResultSet": {
                "Rows": [
                    {"Data": [{"VarCharValue": c} for c in self._columns]},
                    *(
                        {"Data": [({} if v is None else {"VarCharValue": v}) for v in row]}
                        for row in self._rows
                    ),
                ]
            }
        }


class _FakeAthena:
    """Answers the stage's three reads and records every statement it was given."""

    def __init__(
        self,
        *,
        enrichment: list[list[str | None]] | None = None,
        attempts: list[list[str | None]] | None = None,
    ) -> None:
        self.enrichment = enrichment or []
        self.attempts = attempts or []
        self.queries: list[str] = []
        self._current = ""

    def start_query_execution(self, **kwargs: Any) -> dict[str, Any]:
        self._current = kwargs["QueryString"]
        self.queries.append(self._current)
        return {"QueryExecutionId": "x"}

    def get_query_execution(self, QueryExecutionId: str) -> dict[str, Any]:
        del QueryExecutionId
        return {
            "QueryExecution": {
                "Status": {"State": "SUCCEEDED"},
                "Statistics": {"DataScannedInBytes": 0, "EngineExecutionTimeInMillis": 1},
            }
        }

    def get_paginator(self, operation_name: str) -> _Paginator:
        assert operation_name == "get_query_results"
        if "information_schema.columns" in self._current:
            # Every declared column already present, so `ensure_tables` never ALTERs.
            names = [
                [name]
                for ddl in (store.CLUSTER_ENRICHMENT_DDL, store.ENRICHMENT_REJECTS_DDL)
                for name, _ in store._ddl_columns(ddl)
            ]
            return _Paginator(COLUMN_COLUMNS, names)
        if "gold.enrichment_rejects" in self._current:
            return _Paginator(ATTEMPT_COLUMNS, self.attempts)
        if "gold.cluster_enrichment" in self._current:
            return _Paginator(ENRICHMENT_COLUMNS, self.enrichment)
        return _Paginator([], [])

    def inserts_into(self, table: str) -> list[str]:
        return [q for q in self.queries if q.startswith(f"INSERT INTO {table}")]


def _cluster(cluster_id: str = "c1", title: str = "Northwind acquires Lumen", **over):
    return {
        "cluster_id": cluster_id,
        "title": title,
        "publisher_domain": "reuters.com",
        "snippet": "Northwind said it had agreed to buy Lumen Robotics.",
        **over,
    }


def _answer_route(*bodies: str):
    """One `/api/generate` route returning the given raw strings in order."""
    responses = [httpx.Response(200, json={"response": b, "model": "llama3.1:8b"}) for b in bodies]
    return respx.post(f"{BASE}/api/generate").mock(side_effect=responses)


def _key(cluster: dict[str, Any]) -> str:
    return _cache_keys([cluster], SETTINGS)[cluster["cluster_id"]]


# --- the cache ----------------------------------------------------------------------------


@respx.mock
def test_a_cluster_that_already_has_a_row_is_not_re_inferred():
    """The whole point of the cache: SPEC §7.3 says "never re-infer". A second morning over
    an unchanged story must cost zero tokens."""
    cluster = _cluster()
    athena = _FakeAthena(
        enrichment=[[cluster["cluster_id"], _key(cluster), "old summary", "ai-ml", "{}"]]
    )
    route = _answer_route(json.dumps(ANSWER))

    result = enrich_clusters(
        [cluster], settings=SETTINGS, now=NOW, client=athena, schema_format=False
    )

    assert route.call_count == 0, "the model was called for a story already in the cache"
    assert result.cache_hits == 1
    assert result.inferred == 0
    assert result.cache_hit_rate == 1.0
    assert athena.inserts_into(store.CLUSTER_ENRICHMENT_TABLE) == []


@respx.mock
def test_an_edited_headline_is_a_miss_because_the_key_is_the_text_not_the_id():
    """Hashing the head text rather than the cluster id is what makes an edited headline
    re-infer — which is correct, because a headline changing is exactly when a story is
    developing."""
    stored = _cluster(title="Northwind acquires Lumen")
    edited = _cluster(title="Northwind acquires Lumen Robotics for $2bn")
    athena = _FakeAthena(
        enrichment=[[stored["cluster_id"], _key(stored), "old", "business-corporate", "{}"]]
    )
    route = _answer_route(json.dumps(ANSWER))

    result = enrich_clusters(
        [edited], settings=SETTINGS, now=NOW, client=athena, schema_format=False
    )

    assert route.call_count == 1
    assert result.inferred == 1


@respx.mock
def test_two_clusters_with_identical_head_text_cost_one_inference():
    """Syndication produces this. The second cluster gets its own row so the table stays
    queryable per cluster, flagged `cache_hit` — the one kind of hit a stored row can
    honestly record."""
    first, second = _cluster("c1"), _cluster("c2")
    athena = _FakeAthena()
    route = _answer_route(json.dumps(ANSWER))

    result = enrich_clusters(
        [first, second], settings=SETTINGS, now=NOW, client=athena, schema_format=False
    )

    assert route.call_count == 1, "identical text was inferred twice"
    assert result.inferred == 1
    assert result.cache_hits == 1
    inserted = athena.inserts_into(store.CLUSTER_ENRICHMENT_TABLE)[0]
    assert "true" in inserted and "false" in inserted, "one fresh row and one copied row"


@respx.mock
def test_the_cache_read_is_scoped_to_the_pinned_digest_and_prompt_version():
    """Reading on the hash alone would serve output the current configuration would not have
    produced, and would make the hit rate a statistic about the past."""
    athena = _FakeAthena()
    _answer_route(json.dumps(ANSWER))
    enrich_clusters([_cluster()], settings=SETTINGS, now=NOW, client=athena, schema_format=False)

    read = next(q for q in athena.queries if "SELECT cluster_id, input_hash" in q)
    assert "sha256:pinned" in read
    assert "'v1'" in read


# --- validation and quarantine ------------------------------------------------------------


@respx.mock
def test_unparseable_output_is_quarantined_not_stored_and_not_dropped():
    """SPEC §7.3: "quarantined to `gold.enrichment_rejects`, never silently dropped". An
    absent row and a refused row are different facts, and only one of them is debuggable."""
    athena = _FakeAthena()
    _answer_route("not json at all")

    result = enrich_clusters(
        [_cluster()], settings=SETTINGS, now=NOW, client=athena, schema_format=False
    )

    assert result.rejected == 1
    assert result.written == 0
    assert athena.inserts_into(store.CLUSTER_ENRICHMENT_TABLE) == []
    assert len(athena.inserts_into(store.ENRICHMENT_REJECTS_TABLE)) == 1


@respx.mock
def test_a_topic_off_the_list_is_quarantined_rather_than_written():
    """Schema-valid JSON with an unaccepted enum value is the realistic failure, not
    malformed bytes."""
    athena = _FakeAthena()
    _answer_route(json.dumps({**ANSWER, "topic": "artificial-intelligence"}))

    result = enrich_clusters(
        [_cluster()], settings=SETTINGS, now=NOW, client=athena, schema_format=False
    )

    assert result.rejected == 1
    assert "artificial-intelligence" in athena.inserts_into(store.ENRICHMENT_REJECTS_TABLE)[0]


@respx.mock
def test_the_quarantine_row_carries_what_the_model_actually_said():
    """A reject nobody can read is a counter, not a diagnosis."""
    athena = _FakeAthena()
    _answer_route("{'almost': 'json'}")
    enrich_clusters([_cluster()], settings=SETTINGS, now=NOW, client=athena, schema_format=False)

    assert "almost" in athena.inserts_into(store.ENRICHMENT_REJECTS_TABLE)[0]


@respx.mock
def test_retries_stop_at_the_bound_rather_than_costing_an_inference_every_morning():
    """SPEC §7.3: "never retried indefinitely". A head that makes the model emit garbage
    tends to keep doing so; without a bound it bills forever."""
    cluster = _cluster()
    athena = _FakeAthena(attempts=[[_key(cluster), str(store.MAX_ATTEMPTS)]])
    route = _answer_route(json.dumps(ANSWER))

    result = enrich_clusters(
        [cluster], settings=SETTINGS, now=NOW, client=athena, schema_format=False
    )

    assert route.call_count == 0
    assert result.skipped_exhausted == 1
    assert result.inferred == 0


@respx.mock
def test_a_cluster_below_the_retry_bound_is_still_attempted():
    """The bound has to be a ceiling, not a tripwire: one bad decode should not condemn a
    story permanently."""
    cluster = _cluster()
    athena = _FakeAthena(attempts=[[_key(cluster), "1"]])
    route = _answer_route(json.dumps(ANSWER))

    result = enrich_clusters(
        [cluster], settings=SETTINGS, now=NOW, client=athena, schema_format=False
    )

    assert route.call_count == 1
    assert result.written == 1


@respx.mock
def test_the_reject_row_increments_the_attempt_count():
    """The bound lives in the table so it survives a restart. A count that never advances is
    a bound that never binds."""
    cluster = _cluster()
    athena = _FakeAthena(attempts=[[_key(cluster), "1"]])
    _answer_route("garbage")
    enrich_clusters([cluster], settings=SETTINGS, now=NOW, client=athena, schema_format=False)

    assert athena.inserts_into(store.ENRICHMENT_REJECTS_TABLE)[0].rstrip().endswith("2)")


# --- the server being off -----------------------------------------------------------------


@respx.mock
def test_an_unreachable_server_stops_the_batch_instead_of_quarantining_forty_times():
    """The server being off is one operational fact, not forty validation failures. Forty
    quarantine rows about it would bury the real rejects the table exists to surface."""
    athena = _FakeAthena()
    respx.post(f"{BASE}/api/generate").mock(side_effect=httpx.ConnectError("refused"))

    result = enrich_clusters(
        [_cluster("c1"), _cluster("c2", title="Second"), _cluster("c3", title="Third")],
        settings=SETTINGS,
        now=NOW,
        client=athena,
        schema_format=False,
    )

    assert result.unavailable is not None
    assert result.rejected == 0
    assert athena.inserts_into(store.ENRICHMENT_REJECTS_TABLE) == []


@respx.mock
def test_what_was_inferred_before_the_server_died_is_still_written():
    """A batch that loses the server halfway must not throw away the work it already paid
    for — those tokens are spent either way."""
    athena = _FakeAthena()
    respx.post(f"{BASE}/api/generate").mock(
        side_effect=[
            httpx.Response(200, json={"response": json.dumps(ANSWER), "model": "m"}),
            httpx.ConnectError("refused"),
        ]
    )

    result = enrich_clusters(
        [_cluster("c1"), _cluster("c2", title="Second story entirely")],
        settings=SETTINGS,
        now=NOW,
        client=athena,
        schema_format=False,
    )

    assert result.inferred == 1
    assert result.written == 1
    assert result.unavailable is not None


def test_an_empty_window_is_a_rate_of_zero_not_a_division_by_zero():
    """A morning with no clusters is a real state — the cluster DAG has not run — and the
    footer prints this number beside a story count, so a crash here would take down a brief
    that had something honest to say."""
    result = enrich_clusters([], settings=SETTINGS, now=NOW, client=_FakeAthena())
    assert result.cache_hit_rate == 0.0
    assert result.processed == 0


# --- what the brief reads -----------------------------------------------------------------


def test_the_brief_gets_enrichment_keyed_by_cluster_id():
    cluster = _cluster()
    athena = _FakeAthena(
        enrichment=[
            [
                cluster["cluster_id"],
                _key(cluster),
                "A one sentence summary.",
                "business-corporate",
                '{"company": "Northwind"}',
            ]
        ]
    )
    found, query = read_for_clusters([cluster], settings=SETTINGS, client=athena)

    assert found["c1"].summary == "A one sentence summary."
    assert found["c1"].extraction["company"] == "Northwind"
    assert query.bytes_scanned == 0


def test_a_story_whose_headline_changed_comes_back_unenriched_rather_than_stale():
    """Showing yesterday's summary under today's headline is the failure this lookup is
    shaped to prevent — it would be wrong in the most confident-looking way."""
    stored = _cluster(title="Original headline")
    edited = _cluster(title="Completely different headline")
    athena = _FakeAthena(enrichment=[[stored["cluster_id"], _key(stored), "old", "ai-ml", "{}"]])
    found, _ = read_for_clusters([edited], settings=SETTINGS, client=athena)
    assert found == {}


def test_a_row_whose_extraction_json_is_corrupt_is_skipped_not_served():
    """One re-inference is strictly better than handing the brief a value this stage cannot
    read."""
    cluster = _cluster()
    athena = _FakeAthena(
        enrichment=[[cluster["cluster_id"], _key(cluster), "s", "ai-ml", "{not json"]]
    )
    found, _ = read_for_clusters([cluster], settings=SETTINGS, client=athena)
    assert found == {}


def test_no_clusters_means_no_query_at_all():
    """An empty IN list is not a query worth Athena's 10 MB minimum."""
    athena = _FakeAthena()
    found, _ = read_for_clusters([], settings=SETTINGS, client=athena)
    assert found == {}
    assert athena.queries == []


# --- the input ----------------------------------------------------------------------------


def test_the_enriched_text_carries_the_title_and_the_publisher():
    """Both matter to the label: `sec.gov` is most of what tells a routine filing apart from
    business news, and the topic list leans on exactly that."""
    rendered = cluster_input(_cluster())
    assert "Northwind acquires Lumen" in rendered
    assert "reuters.com" in rendered


@pytest.mark.parametrize("missing", ["title", "publisher_domain", "snippet"])
def test_a_cluster_missing_a_field_still_renders_an_input(missing: str):
    """`read_clusters` coerces nulls to empty strings, but a None reaching here would render
    as the literal word "None" and become a content word the model sees."""
    cluster = _cluster()
    cluster[missing] = None
    assert "None" not in cluster_input(cluster)


def test_run_refuses_an_unpinned_digest():
    """ADR-0003's gate, enforced in the library rather than only in the CLI — the enrich DAG
    calls `run` directly, and a guard only one of two entry points enforces is not a guard.

    Every row written under `UNPINNED` would be keyed on a string that does not name a model,
    so it could never legitimately be served: `read_cached` would miss on it forever, quietly
    re-inferring the same heads every morning while reporting a hit rate of zero.
    """
    from signal_core.enrich.run import run

    unpinned = SETTINGS.model_copy(update={"ollama_model_digest": "UNPINNED"})
    with pytest.raises(RuntimeError, match="unpinned"):
        run(settings=unpinned, client=_FakeAthena())


def test_the_gold_enrichment_ddl_uses_types_athena_accepts():
    """Same guard as `test_brief_items.py`'s, for the two tables 4B adds. The fake Athena
    client records SQL without parsing it, so a type Athena rejects looks identical to one it
    accepts until the first real run — which is how `gold.brief_items` shipped in 4A without
    ever existing."""
    from tests.test_brief_items import ATHENA_ICEBERG_TYPES, _declared_types

    for ddl in (store.CLUSTER_ENRICHMENT_DDL, store.ENRICHMENT_REJECTS_DDL):
        for declared in _declared_types(ddl):
            assert declared in ATHENA_ICEBERG_TYPES, f"{declared!r} is not an Athena Iceberg type"


def test_run_enriches_only_the_ranked_cut_not_the_whole_window(monkeypatch):
    """The bug this test exists for, found by a run that took 70 minutes instead of one.

    `ranker.rank` returns **every** scored cluster with only the top `limit` flagged
    `included` — it *marks* the cut, it does not apply it. `run` passed `window.clusters`
    straight through, so a 2,979-cluster window sent 2,979 heads to the GPU instead of 40,
    spending most of the budget on exactly the routine SEC filings ADR-0011 exists to keep
    out. Nothing else would have caught it: every count in `EnrichmentRun` was internally
    consistent, and the rows written were individually correct.
    """
    from signal_core.brief import select as select_module
    from signal_core.enrich import run as run_module

    window = [_cluster(f"c{i}", title=f"Story number {i}") for i in range(50)]
    for position, cluster in enumerate(window, start=1):
        cluster["rank"] = position
        cluster["included"] = position <= 10

    monkeypatch.setattr(
        run_module,
        "ranked_window",
        lambda **_: select_module.RankedWindow(
            clusters=window,
            cluster_read=SimpleNamespace(clusters=window),
            entities={},
            velocity_slopes={},
            market_moves={},
            feedback={},
            cluster_query=store.EMPTY_RESULT,
            entity_query=store.EMPTY_RESULT,
            velocity_query=store.EMPTY_RESULT,
            market_query=store.EMPTY_RESULT,
            feedback_query=store.EMPTY_RESULT,
        ),
    )

    seen: list[list[dict]] = []
    monkeypatch.setattr(
        run_module,
        "enrich_clusters",
        lambda clusters, **_: seen.append(clusters) or EnrichmentRun(processed=len(clusters)),
    )

    result = run_module.run(settings=SETTINGS, client=_FakeAthena())

    assert len(seen[0]) == 10, "enriched the whole window instead of the ranked cut"
    assert result.processed == 10
    assert all(c["included"] for c in seen[0])


@respx.mock
def test_a_fully_cached_run_never_touches_the_model():
    """The steady state this stage is designed for, and the one the 06:15 DAG hits every
    morning after the first.

    The structured-output probe is a *real* generation, so on a cold GPU it pays ADR-0003's
    ~22.5 s model load. Running it eagerly made a run with nothing to infer cost 16.5 s to
    infer nothing — a cache that still pays for a model load has given back most of what it
    saves. Measured, not assumed: that was the second real run against the deployed lake.
    """
    cluster = _cluster()
    athena = _FakeAthena(
        enrichment=[[cluster["cluster_id"], _key(cluster), "cached", "ai-ml", "{}"]]
    )
    probe = respx.post(f"{BASE}/api/generate")

    result = enrich_clusters([cluster], settings=SETTINGS, now=NOW, client=athena)

    assert probe.call_count == 0, "a fully cached run still called the model"
    assert result.cache_hit_rate == 1.0
    assert result.inferred == 0
