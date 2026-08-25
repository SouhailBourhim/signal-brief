from __future__ import annotations

from pathlib import Path

from signal_core.watchlist import Watchlist, load


def test_tickers_are_the_uppercase_ids():
    """The namespace does the work. `entities/dictionary.py`: UPPERCASE is a tradable
    ticker, lower-kebab-case is an entity without one — so a private company on the
    watchlist counts for relevance and is not fetched from Stooq, with no join to discover
    that."""
    w = Watchlist(
        companies=frozenset({"NVDA", "AAPL", "openai"}),
        technologies=(),
        macro_series=(),
    )
    assert w.tickers() == {"NVDA", "AAPL"}
    assert w.has_company("openai")


def test_technology_matching_is_substring_and_case_insensitive():
    """`post-training` inside `post-training run` is a match; a token rule would miss it."""
    w = Watchlist(
        companies=frozenset(),
        technologies=("post-training", "gpu"),
        macro_series=(),
    )
    assert w.matched_technologies("A new POST-TRAINING run") == ("post-training",)
    assert w.matched_technologies(None, "cheaper GPU inference") == ("gpu",)
    assert w.matched_technologies("nothing relevant here") == ()
    assert w.matched_technologies(None, None) == ()


def test_matches_are_returned_not_just_counted():
    """SPEC §7.4 wants every ranking decision explainable after the fact, so the component
    reports *which* keywords hit rather than asserting an unexplained score."""
    w = Watchlist(
        companies=frozenset(),
        technologies=("llm", "rust"),
        macro_series=(),
    )
    assert set(w.matched_technologies("An LLM written in Rust")) == {"llm", "rust"}


def test_missing_entity_id_is_not_a_match():
    """`read_cluster_entities` returns nothing for a cluster with no resolved entity, and
    the ranker must score that 0 rather than raising."""
    w = Watchlist(companies=frozenset({"NVDA"}), technologies=(), macro_series=())
    assert not w.has_company(None)
    assert not w.has_company("")


def test_the_committed_watchlist_loads_and_is_usable():
    """The shipped file, not a fixture: a malformed edit to `watchlist.toml` should fail
    here rather than at 16:00."""
    w = load()
    assert w.companies, "watchlist has no companies"
    assert w.tickers(), "watchlist has no tradable tickers for Stooq to fetch"
    assert all(t.islower() for t in w.technologies), "technologies are lowercased at load"
    # Every id is either a ticker or lower-kebab-case. A mixed-case entry like `Nvda` is a
    # typo that would silently never match the dictionary.
    for company in w.companies:
        assert company.isupper() or company.islower(), company


def test_load_is_cached():
    """`lru_cache`, matching `entities/dictionary.py::load`. Called once per cluster per
    brief, over a file that never changes mid-run."""
    assert load() is load()


def test_a_watchlist_can_be_loaded_from_an_explicit_path(tmp_path: Path):
    """The loader takes a path so a test can point at a fixture without touching the
    shipped file."""
    p = tmp_path / "w.toml"
    p.write_text('companies = ["TSLA"]\ntechnologies = ["Robotaxi"]\n', encoding="utf-8")
    w = load(p)
    assert w.tickers() == {"TSLA"}
    assert w.technologies == ("robotaxi",)
    assert w.macro_series == ()
