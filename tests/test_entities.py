"""Entity dictionary and resolver. SPEC §7.2.

Every resolution test runs against a **hand-built dictionary**, not the committed snapshot.
Two reasons: a test that depends on what SEC listed the morning the snapshot was built fails
for a reason that has nothing to do with the code, and a fixture of eight entities makes the
case each test is about legible. The snapshot gets its own test — that it loads, and that its
shape is what `resolve` assumes.
"""

from __future__ import annotations

import pytest

from signal_core.entities import dictionary as dict_module
from signal_core.entities.dictionary import Entity
from signal_core.entities.resolve import (
    UNLINKED_NOT_A_COMPANY,
    Resolution,
    expand,
    resolve,
)


@pytest.fixture
def dictionary():
    """Eight entities, each carrying one of the cases the resolver has to separate."""
    return dict_module.build(
        [
            Entity("AAPL", "Apple Inc.", "public", "sec", ticker="AAPL", cik="0000320193", rank=1),
            # Shares the `apple` prefix with AAPL, and is far less prominent.
            Entity("APLE", "Apple Hospitality REIT, Inc.", "public", "sec", rank=900),
            Entity("CMCSA", "COMCAST CORP", "public", "sec", ticker="CMCSA", rank=194),
            # A name prose never says in full — the prefix channel's reason to exist.
            Entity("GETY", "Getty Images Holdings, Inc.", "public", "sec", rank=3000),
            Entity("QTRX", "Quanterix Corp", "public", "sec", ticker="QTRX", cik="0001503274"),
            # A surname that starts a company name. The person veto's target.
            Entity("MATW", "Matthews International Corp", "public", "sec", rank=2500),
            Entity("TGT", "Target Corp", "public", "sec", ticker="TGT", rank=120),
            Entity("openai", "OpenAI", "private", "wikidata"),
        ],
        # Ranked by frequency, so index 0 is the commonest word. `target` and `apple` are
        # everyday words; `comcast`, `getty` and `quanterix` are not.
        common_words=["the", "target", "apple", "block", "meta"],
        built_at="2026-08-21T00:00:00+00:00",
    )


def _resolve(surface: str, context: str, dictionary) -> Resolution:
    return resolve(surface, context, dictionary=dictionary)


# --- name normalization ---------------------------------------------------------------


def test_normalize_drops_registry_markers_and_punctuation():
    assert dict_module.normalize("BANK OF MONTREAL /CAN/") == ("bank", "of", "montreal")
    assert dict_module.normalize("Lazard, Inc.") == ("lazard", "inc")


def test_normalize_strips_the_possessive_prose_actually_uses():
    assert dict_module.normalize("Comcast's") == ("comcast",)


def test_legal_suffix_takes_everything_after_it():
    """`JS VENTURE FUND LLC SERIES` is a fund, not a fund called `... llc series`."""
    tokens = dict_module.normalize("JS VENTURE FUND LLC SERIES")
    assert dict_module.strip_legal_suffix(tokens) == ("js", "venture", "fund")


def test_words_that_look_like_suffixes_but_name_the_company_survive():
    """The hand labels kept these: `spotwise-data-group`, `mishpacha-fund`, `chiba-bank`."""
    assert dict_module.slug("SpotWise Data Group LLC") == "spotwise-data-group"
    assert dict_module.slug("Mishpacha Fund, LP") == "mishpacha-fund"
    assert dict_module.slug("Chiba Bank, Ltd.") == "chiba-bank"


def test_a_name_that_is_only_a_suffix_does_not_reduce_to_nothing():
    """Otherwise it would produce an empty alias, which matches everything."""
    assert dict_module.strip_legal_suffix(("co",)) == ("co",)


# --- the alias index ------------------------------------------------------------------


def test_every_prefix_of_a_name_is_an_alias(dictionary):
    """Prose says `Getty Images`; SEC says `Getty Images Holdings, Inc.`."""
    assert dictionary.lookup(("getty",)).starts == ("GETY",)
    assert dictionary.lookup(("getty", "images")).starts == ("GETY",)
    assert dictionary.lookup(("getty", "images", "holdings")).completes == ("GETY",)


def test_a_complete_name_and_a_prefix_of_another_are_recorded_separately(dictionary):
    """`apple` completes Apple Inc. and merely starts Apple Hospitality REIT."""
    alias = dictionary.lookup(("apple",))
    assert alias.completes == ("AAPL",)
    assert alias.starts == ("APLE",)


# --- the four channels ----------------------------------------------------------------


def test_cik_stated_next_to_the_span_wins(dictionary):
    """EDGAR supplies the identifier; reading it is not inference."""
    result = _resolve(
        "Quanterix Corp",
        "8-K - Quanterix Corp (0001503274) (Filer) <b>Filed:</b> 2026-08-18",
        dictionary,
    )
    assert (result.entity_id, result.method) == ("QTRX", "cik")


def test_a_cik_no_company_holds_vetoes_the_surname_behind_it(dictionary):
    """EDGAR Form 4 filers are people, and their surnames start company names.

    Without this, `Matthews Mark E.` resolves through Matthews International — the single
    largest source of false positives the labeled set contains.
    """
    result = _resolve(
        "Matthews Mark E.",
        "4 - Matthews Mark E. (0002018289) (Reporting) <b>Filed:</b> 2026-08-18",
        dictionary,
    )
    assert result.entity_id is None
    assert result.reason == UNLINKED_NOT_A_COMPANY


def test_a_legal_form_overrides_the_veto_and_mints(dictionary):
    """A private fund also holds a CIK no ticker claims — but it is a company."""
    result = _resolve(
        "Mishpacha Fund, LP",
        "D - Mishpacha Fund, LP (0002145446) (Filer) <b>Filed:</b> 2026-08-20",
        dictionary,
    )
    assert (result.entity_id, result.method) == ("mishpacha-fund", "minted")


def test_a_complete_name_resolves(dictionary):
    assert _resolve("Comcast's", "Comcast's earnings call", dictionary).entity_id == "CMCSA"


def test_the_prefix_channel_is_inert_at_the_fitted_floor(dictionary):
    """`Getty Images` finds Getty Images Holdings and then declines to link it.

    Not a bug, and worth pinning so a future change to either number is deliberate:
    `CONFIDENCE_PREFIX` is 0.70 and the fitted `CONFIDENCE_FLOOR` is 0.72, so **no prefix
    match links at the current operating point**. The labeled set is why — admitting them
    was measured as neutral on the train half and slightly negative on the held-out half.
    The index is kept because it is what a lower floor would use once there is evidence
    worth trusting it on, which is what SPEC §7.2's embedding stage would supply.
    """
    result = _resolve("Getty Images", "Getty Images sues an AI lab", dictionary)
    assert result.entity_id is None
    assert result.matched_alias == "getty images", "it found the entity, then declined"


def test_a_prefix_resolves_once_the_floor_allows_it(monkeypatch, dictionary):
    """The channel works; the floor is what silences it."""
    from signal_core.entities import resolve as resolve_module

    monkeypatch.setattr(resolve_module, "CONFIDENCE_FLOOR", 0.65)
    assert _resolve("Getty Images", "Getty Images sues an AI lab", dictionary).entity_id == "GETY"


def test_a_shared_prefix_goes_to_the_more_prominent_company(dictionary):
    """`apple` starts two names, and only one of them is ever in the news.

    SEC's own file ordering breaks the tie — the same judgment that picks `AEG` over
    `AEGOF`. Confidence stays low enough that the floor still has a say.
    """
    from signal_core.entities import resolve as resolve_module

    ranked = resolve_module._candidate(dictionary, ("apple",))
    assert ranked == ("AAPL", "name")


# --- abstention -----------------------------------------------------------------------


def test_the_feeds_own_field_names_are_never_a_company(dictionary):
    """`Filed`, `AccNo` and `Filer` are what a proper-noun heuristic finds in EDGAR."""
    for surface in ("Filed", "AccNo", "Filer"):
        result = _resolve(surface, f"<b>{surface}:</b> 2026-08-18", dictionary)
        assert result.entity_id is None, surface
        assert result.reason == UNLINKED_NOT_A_COMPANY


def test_a_bare_everyday_word_does_not_link(dictionary):
    """SPEC §7.2's own example, which the corpus supplied for real: a river, not a shop."""
    result = _resolve("Apple", "an apple orchard in Kent", dictionary)
    assert result.entity_id is None


def test_a_company_name_buried_in_a_headline_does_not_win(dictionary):
    """`Binance Helped Russia Target` matches `target` — Target Corp — four tokens in.

    Taking the first or the only match linked a crypto story to a retailer.
    """
    result = _resolve(
        "Binance Helped Russia Target", "Binance Helped Russia Target Dissidents", dictionary
    )
    assert result.entity_id != "TGT"


def test_an_unknown_capitalized_string_with_no_legal_form_abstains(dictionary):
    """A person, a product or a headline fragment — the majority of the labeled set."""
    assert (
        _resolve("Curvature Beziers", "Curvature Beziers explained", dictionary).entity_id is None
    )


# --- span expansion -------------------------------------------------------------------


def test_a_span_grows_into_the_name_it_sits_inside():
    """The sampler cuts `PIER 88 INVESTMENT PARTNERS LLC` in two; both halves are labeled
    with the same entity because a reader sees one name."""
    context = "N-PX - PIER 88 INVESTMENT PARTNERS LLC (0001697366) (Filer) <b>Filed:</b> 2026-08-20"
    assert expand("PIER", context) == "PIER 88 INVESTMENT PARTNERS LLC"
    assert expand("INVESTMENT PARTNERS LLC", context) == "PIER 88 INVESTMENT PARTNERS LLC"


def test_expansion_stops_at_lowercase_prose():
    assert expand("Comcast", "regulators said Comcast will appeal") == "Comcast"


def test_both_halves_of_a_split_name_mint_the_same_id(dictionary):
    """Two spans, one company, one id — or `dim_entities` accumulates synonyms of itself."""
    context = "N-PX - PIER 88 INVESTMENT PARTNERS LLC (0001697366) (Filer) <b>Filed:</b> 2026-08-20"
    first = _resolve("PIER", context, dictionary)
    second = _resolve("INVESTMENT PARTNERS LLC", context, dictionary)
    assert first.entity_id == second.entity_id == "pier-88-investment-partners"


# --- the floor ------------------------------------------------------------------------


def test_the_floor_is_applied_to_every_channel(monkeypatch, dictionary):
    """SPEC §7.2's "unlinked rather than guessed", in one place so nothing opts out."""
    from signal_core.entities import resolve as resolve_module

    monkeypatch.setattr(resolve_module, "CONFIDENCE_FLOOR", 1.01)
    assert _resolve("Comcast", "Comcast earnings", dictionary).entity_id is None


def test_resolution_is_deterministic(dictionary):
    """A replay has to reproduce a run exactly, not within a tolerance."""
    calls = [_resolve("Getty Images", "Getty Images sues an AI lab", dictionary) for _ in range(5)]
    assert len({(c.entity_id, c.confidence, c.method) for c in calls}) == 1


# --- the committed snapshot -----------------------------------------------------------


def test_the_committed_dictionary_loads_and_has_the_shape_resolve_assumes():
    """Not an accuracy test — `make eval` is that. This checks the snapshot is present and
    well-formed, so a missing rebuild fails here rather than as a mystery in the eval."""
    loaded = dict_module.load()
    assert loaded.built_at
    assert len(loaded.entities) > 1000
    assert loaded.common_words, "the frequency list is what the common-word channel reads"

    apple = loaded.entities["AAPL"]
    assert (apple.ticker, apple.entity_type) == ("AAPL", "public")
    assert loaded.by_cik[apple.cik] == "AAPL"


def test_one_company_with_many_tickers_keeps_its_primary_listing():
    """SEC lists `BANK OF MONTREAL /CAN/` 32 times, once per structured note it issues.

    The hand labels say `BMO`, and the lowest index in SEC's prominence-ordered file is how
    that is known without anybody deciding it here.
    """
    loaded = dict_module.load()
    montreal = [e for e in loaded.entities.values() if e.cik == "0000927971"]
    assert [e.entity_id for e in montreal] == ["BMO"]
