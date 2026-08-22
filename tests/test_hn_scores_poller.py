from __future__ import annotations

import httpx
import pytest
import respx

from signal_core.config import SOURCES
from signal_core.contracts import FetchOutcome, State
from signal_core.sources import hn_scores


@pytest.fixture
def config():
    return SOURCES["hn_scores"]


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(hn_scores.time, "sleep", lambda _seconds: None)


def _mock_top(config, ids: list[int]) -> None:
    respx.get(f"{config.url}/topstories.json").mock(return_value=httpx.Response(200, json=ids))
    for item_id in ids:
        respx.get(f"{config.url}/item/{item_id}.json").mock(
            return_value=httpx.Response(
                200, json={"id": item_id, "type": "story", "score": 10, "title": "t"}
            )
        )


@respx.mock
def test_it_fetches_the_ranked_ids_rather_than_walking_a_frontier(config):
    _mock_top(config, [11, 22, 33])

    documents, state = hn_scores.poll(config, State(source_id=config.source_id))

    assert [d.source_url for d in documents] == [
        f"{config.url}/item/{i}.json" for i in (11, 22, 33)
    ]
    assert all(d.outcome == FetchOutcome.OK for d in documents)
    # No watermark. There is no frontier here — the ranking is a set that reshuffles, and
    # a watermark would quietly stop this source re-reading the ids it exists to re-read.
    assert state.watermark is None


@respx.mock
def test_the_same_ids_are_re_fetched_on_every_poll(config):
    """The whole point, and the opposite of `sources/hackernews.py`. A poller that skipped
    ids it had already seen would produce one point per story and no slope — which is the
    defect SPEC §12 carried into 4A."""
    _mock_top(config, [11, 22])

    first_docs, first_state = hn_scores.poll(config, State(source_id=config.source_id))
    second_docs, _ = hn_scores.poll(config, first_state)

    assert [d.source_url for d in first_docs] == [d.source_url for d in second_docs]
    # Distinct observations, so `commit_bronze`'s MERGE on ingest_id keeps both rather
    # than collapsing the second into the first.
    assert {d.ingest_id for d in first_docs}.isdisjoint({d.ingest_id for d in second_docs})


@respx.mock
def test_only_the_top_n_are_sampled(config, monkeypatch):
    """~500 ids every 15 minutes is 48,000 requests a day to watch the tail of a ranking
    that will never lead a brief."""
    monkeypatch.setattr(hn_scores, "TOP_N", 2)
    _mock_top(config, [11, 22, 33, 44])

    documents, _ = hn_scores.poll(config, State(source_id=config.source_id))

    assert [d.source_url for d in documents] == [f"{config.url}/item/{i}.json" for i in (11, 22)]


@respx.mock
def test_a_static_ranking_is_content_staleness_not_fetch_staleness(config):
    """SPEC §11 and 1.E: a source can succeed and still be dead. The hash is over the ranked
    id list rather than any item, because scores drift constantly and would report movement
    even from a frozen API — while the *ranking* going static is the real signal."""
    _mock_top(config, [11, 22])

    _, first = hn_scores.poll(config, State(source_id=config.source_id))
    _, second = hn_scores.poll(config, first)

    assert first.last_content_change_at is not None
    # Same ranking, so content did not move — but the fetch did succeed.
    assert second.last_content_change_at == first.last_content_change_at
    assert second.last_success_at > first.last_success_at


@respx.mock
def test_a_reshuffled_ranking_registers_as_movement(config):
    _mock_top(config, [11, 22])
    _, first = hn_scores.poll(config, State(source_id=config.source_id))

    respx.get(f"{config.url}/topstories.json").mock(return_value=httpx.Response(200, json=[22, 11]))
    _, second = hn_scores.poll(config, first)

    assert second.last_content_change_at > first.last_content_change_at


@respx.mock
def test_an_unreachable_top_list_counts_a_failure_and_emits_nothing(config):
    respx.get(f"{config.url}/topstories.json").mock(return_value=httpx.Response(500))

    documents, state = hn_scores.poll(config, State(source_id=config.source_id))

    assert documents == []
    assert state.consecutive_failures == 1
    assert state.last_success_at is None


@respx.mock
def test_one_bad_item_does_not_cost_the_others(config):
    """SPEC §6.2: a failed fetch becomes an ERROR document, never an escaped exception."""
    respx.get(f"{config.url}/topstories.json").mock(return_value=httpx.Response(200, json=[11, 22]))
    respx.get(f"{config.url}/item/11.json").mock(return_value=httpx.Response(500))
    respx.get(f"{config.url}/item/22.json").mock(
        return_value=httpx.Response(200, json={"id": 22, "type": "story", "score": 5})
    )

    documents, state = hn_scores.poll(config, State(source_id=config.source_id))

    assert [d.outcome for d in documents] == [FetchOutcome.ERROR, FetchOutcome.OK]
    assert state.consecutive_failures == 0


@respx.mock
def test_a_deleted_id_is_empty_not_an_error(config):
    respx.get(f"{config.url}/topstories.json").mock(return_value=httpx.Response(200, json=[11]))
    respx.get(f"{config.url}/item/11.json").mock(return_value=httpx.Response(200, text="null"))

    documents, _ = hn_scores.poll(config, State(source_id=config.source_id))

    assert documents[0].outcome == FetchOutcome.EMPTY
