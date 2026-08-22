"""Settings and the source registry.

The registry is the single place a source becomes real. Adding source #6 is: write the
module, add one entry here, add one Terraform map entry. SPEC §3, §6.1.
"""

from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from signal_core.contracts import BackfillHorizon, PayloadFormat, SourceConfig


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SIGNAL_", env_file=".env", extra="ignore")

    contact_email: str = "you@example.com"
    data_root: Path = Path("./data")
    out_root: Path = Path("./out")
    cache_root: Path = Path("./.cache")
    ollama_url: str = "http://localhost:11434"

    # Pinned, not floating. SPEC §7.3: swapping a model is a measurement, not a vibe.
    ollama_model: str = "llama3.1:8b"
    ollama_model_digest: str = "UNPINNED"
    prompt_version: str = "v0"

    aws_region: str = Field(default="us-east-1", alias="AWS_REGION")

    # Phase 1 ingestion infra (Terraform-provisioned; see infra/terraform/main).
    state_table_name: str = "signal-pipeline-state"
    bronze_bucket: str = "signal-bronze"

    # Iceberg. `iceberg_warehouse` doubles as the catalog switch: an s3:// URI selects
    # Glue + S3FileIO, anything else a local Hadoop catalog. See spark/session.py.
    iceberg_catalog: str = "signal"
    iceberg_warehouse: str = ""

    # Phase 2 querying (infra/terraform/main/query.tf). `enforce_workgroup_configuration`
    # on the Terraform side means the workgroup's own settings — including the bytes-
    # scanned cutoff — win regardless of what a client requests.
    athena_workgroup: str = "signal"
    athena_database: str = "silver"

    @property
    def user_agent(self) -> str:
        """`Name contact@email` — the shape SEC actually accepts. SPEC §6.2.

        Not a style preference. EDGAR 403s the conventional browser-ish form
        (`signal/0.0 (+https://github.com/signal; addr)`) with "Your Request Originates
        from an Undeclared Automated Tool", and serves a 200 for the plain name-then-email
        form. Measured against the live endpoint on 2026-08-18, not inferred from the
        docs. The other sources accept either, so there is one User-Agent, in the format
        the strictest source demands.
        """
        return f"Signal Brief {self.contact_email}"

    @property
    def bronze_root(self) -> Path:
        return self.data_root / "bronze"

    @property
    def bronze_staging_uri(self) -> str:
        """Where Lambda pollers land raw objects. A local Spark job — not this handler,
        SPEC §4's "processing is local" boundary — commits them into the
        `bronze.raw_documents` Iceberg table on its own schedule (see
        `spark/jobs/commit_bronze.py`)."""
        return f"s3://{self.bronze_bucket}/staging"

    @property
    def staging_cache_root(self) -> Path:
        """Where staged objects are mirrored before Spark reads them. Under `cache_root`
        because it is exactly SPEC §6.2's read-once cache: staged objects are immutable,
        so one download each, ever (SPEC §10.1)."""
        return self.cache_root / "staging"

    @property
    def warehouse_uri(self) -> str:
        """Iceberg warehouse root. Defaults to a local directory so a fresh clone can run
        the commit job and the Phase 1 acceptance test without an AWS account."""
        return self.iceberg_warehouse or str(self.data_root / "warehouse")

    @property
    def silver_root(self) -> Path:
        return self.data_root / "silver"


settings = Settings()

# `min_docs_per_window` counts **bronze documents per hour**, not feed items, and the
# right value therefore depends on whether the source uses conditional GET — a 304 yields
# no document at all. Measured against 41 hours of real production data, 2026-08-20:
#
# | source      | docs/hour observed | conditional GET | floor | dead-feed SLA |
# |-------------|--------------------|-----------------|-------|---------------|
# | hackernews  | 119-919            | n/a (id walk)   | 50    | 1 h           |
# | edgar       | 4 (steady)         | none served     | 1     | 96 h          |
# | edgar_formd | 4 (steady)         | none served     | 1     | 96 h          |
# | rss_tech    | 0-4, many zeros    | ETag            | 0     | 48 h          |
# | rss_verge   | 0-4, many zeros    | weak ETag       | 0     | 48 h          |
# | rss_ars     | 0-1, mostly zero   | Last-Modified   | 0     | 72 h          |
#
# The two SEC sources serve no validators (`browse-edgar` is a CGI script), so every poll
# is a full body and a floor of 1 means "the poller ran". The RSS sources mostly 304, so
# zero documents in an hour is their normal reading and a floor above 0 fires constantly —
# which is why dead-feed detection, not the floor, is what covers them. SPEC §11.
SOURCES: dict[str, SourceConfig] = {
    # Phase 0 — no network, no AWS. Exists so the contract has a second implementer
    # before the real ones are written.
    "fake": SourceConfig(
        source_id="fake",
        url="memory://fake",
        payload_format=PayloadFormat.JSON,
        backfill_horizon=BackfillHorizon.COMPLETE,
        freshness_sla_seconds=3600,
        min_docs_per_window=1,
        user_agent=settings.user_agent,
    ),
    # Phase 1 — three real sources. SPEC §3: one tech RSS feed, Hacker News, SEC EDGAR
    # current filings.
    "hackernews": SourceConfig(
        source_id="hackernews",
        url="https://hacker-news.firebaseio.com/v0",
        payload_format=PayloadFormat.JSON,
        # Sequential item ids make every item addressable forever. SPEC §3.
        backfill_horizon=BackfillHorizon.COMPLETE,
        # 3x the deployed cadence (rate(5 minutes) in infra/terraform/main). An SLA
        # shorter than the poll interval reports every source as permanently stale, which
        # trains the alert away — the failure mode SPEC §11 is trying to avoid, arrived at
        # from the other direction. Change this and var.sources together.
        freshness_sla_seconds=900,
        # Was 0, commented "a quiet minute on HN is normal, not a failure" — true of a
        # minute, but the assessment window is an *hour* (`monitor.window_bounds`), and
        # HN runs 119-919 documents an hour in production. A floor of 0 meant the
        # highest-volume source in the pipeline could go completely silent and report
        # `ok`. 50 is well under the observed floor and still unmistakably "dead".
        min_docs_per_window=50,
        # HN emits continuously; an hour with no new item ids at all is a dead API, not
        # a quiet patch. This is the one source where content movement is fast enough for
        # the content SLA to be short.
        content_staleness_sla_seconds=3600,
        rate_limit_per_sec=5.0,
        # Short, because one poll is up to 200 of these: a generous per-request timeout
        # multiplies into a killed invocation, and a slow item is not worth waiting for
        # when the next poll re-reads the same watermark anyway.
        timeout_seconds=5.0,
        user_agent=settings.user_agent,
    ),
    "edgar": SourceConfig(
        source_id="edgar",
        url="https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&output=atom",
        payload_format=PayloadFormat.XML,
        # Current feed only recovers ~1 day; the daily index fallback is Phase 2+. SPEC §3.
        backfill_horizon=BackfillHorizon.DAY,
        freshness_sla_seconds=2700,  # 3x rate(15 minutes)
        # Serves no validators, so every poll is a full body: 4 documents an hour,
        # steady. Zero means the poller broke, not that SEC was quiet.
        min_docs_per_window=1,
        # 96 h, not 45 min: SEC files on business days only, so the longest legitimate
        # silence is a holiday weekend — Thursday-Friday closed puts ~4 days between the
        # last filing and the next. The check is for a *permanently* frozen feed, so it
        # sits past the longest legitimate gap rather than near the average one; firing
        # every Thanksgiving is how an alert gets trained away (SPEC §11).
        content_staleness_sla_seconds=345600,
        rate_limit_per_sec=1.0,  # SEC fair-access limits; a descriptive User-Agent is required
        # browse-edgar is a CGI script, not a static file, and 10s was not enough from
        # Lambda — measured, not guessed.
        timeout_seconds=30.0,
        user_agent=settings.user_agent,
    ),
    "rss_tech": SourceConfig(
        source_id="rss_tech",
        url="https://techcrunch.com/feed/",
        payload_format=PayloadFormat.XML,
        # Only what is still in the feed survives an outage. SPEC §3, §6.3.
        backfill_horizon=BackfillHorizon.WINDOW,
        freshness_sla_seconds=2700,  # 3x rate(15 minutes)
        # 0, not 1: this feed serves an ETag and mostly 304s, so most hours produce no
        # document at all. A floor of 1 reported a perfectly healthy feed as `thin` on
        # every quiet hour — the alert-training failure §11 warns about.
        min_docs_per_window=0,
        content_staleness_sla_seconds=172800,  # 48 h without a new item is a dead feed
        rate_limit_per_sec=1.0,
        user_agent=settings.user_agent,
    ),
    # Phase 2 — three more sources. SPEC §3 asks for a second RSS publisher and SEC
    # Form D; the third (`rss_ars`) is deliberately source #6, the one §3's "adding
    # source #6 must be a 30-minute job" claim is measured against.
    "edgar_formd": SourceConfig(
        source_id="edgar_formd",
        url="https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=D&output=atom",
        payload_format=PayloadFormat.XML,
        # DAY, not COMPLETE. SPEC §3 lists Form D as complete — true of the daily
        # full-index files, not of this current-filings feed. Claiming COMPLETE would make
        # plan_catch_up promise a recovery it cannot perform and suppress the gap_reason
        # §6.3 exists to surface. See sources/edgar_formd.py.
        backfill_horizon=BackfillHorizon.DAY,
        freshness_sla_seconds=2700,  # 3x rate(15 minutes)
        min_docs_per_window=1,  # unconditional, same as `edgar`
        content_staleness_sla_seconds=345600,  # 96 h — SEC holiday weekends, as above
        rate_limit_per_sec=1.0,  # shares SEC's per-IP fair-access budget with `edgar`
        timeout_seconds=30.0,  # the same browse-edgar CGI script that needed it for `edgar`
        user_agent=settings.user_agent,
    ),
    "rss_verge": SourceConfig(
        source_id="rss_verge",
        url="https://www.theverge.com/rss/index.xml",
        payload_format=PayloadFormat.XML,
        backfill_horizon=BackfillHorizon.WINDOW,
        freshness_sla_seconds=2700,  # 3x rate(15 minutes)
        min_docs_per_window=0,  # weak ETag, mostly 304s — see the table above
        content_staleness_sla_seconds=172800,  # 48 h
        rate_limit_per_sec=1.0,
        user_agent=settings.user_agent,
    ),
    "rss_ars": SourceConfig(
        source_id="rss_ars",
        url="https://feeds.arstechnica.com/arstechnica/index",
        payload_format=PayloadFormat.XML,
        backfill_horizon=BackfillHorizon.WINDOW,
        freshness_sla_seconds=2700,  # 3x rate(15 minutes)
        # The lowest-volume source: 6 documents across 41 measured hours, because it
        # serves `Last-Modified` and 304s nearly every poll. Its healthy state is zero.
        min_docs_per_window=0,
        content_staleness_sla_seconds=259200,  # 72 h, matching its lower publish rate
        rate_limit_per_sec=1.0,
        user_agent=settings.user_agent,
    ),
    # Phase 4A — SPEC §7.4's velocity component. See sources/hn_scores.py for why this is a
    # separate source rather than a mode of `hackernews`.
    "hn_scores": SourceConfig(
        source_id="hn_scores",
        url="https://hacker-news.firebaseio.com/v0",
        payload_format=PayloadFormat.JSON,
        # WINDOW, not COMPLETE — the opposite of `hackernews`, from the same API. That
        # source's horizon is COMPLETE because item ids are addressable forever; this one
        # samples *the ranking*, which is a set that reshuffles and is gone once it has.
        # A missed poll is a missing point on a slope and no later fetch recovers it.
        backfill_horizon=BackfillHorizon.WINDOW,
        freshness_sla_seconds=2700,  # 3x rate(15 minutes)
        # One poll emits TOP_N documents, every poll, unconditionally — this source has no
        # 304 and no "nothing new" state. Four polls an hour at TOP_N=60 is 240; a floor
        # well under that catches a partially failing poll, not just a dead one.
        min_docs_per_window=180,
        # The ranked id list going static for an hour is a dead API. HN's front page turns
        # over continuously — this is the same reasoning as `hackernews`'s 3600.
        content_staleness_sla_seconds=3600,
        rate_limit_per_sec=5.0,
        timeout_seconds=5.0,
        user_agent=settings.user_agent,
    ),
    # Phase 4A — SPEC §7.4's market-corroboration component. ADR-0010 records why this is a
    # bare JSON endpoint rather than yfinance (pandas) or Stooq (browser challenge).
    "market": SourceConfig(
        source_id="market",
        url="https://query1.finance.yahoo.com",
        payload_format=PayloadFormat.JSON,
        # Every fetch re-states ~63 trading days, so any past bar is re-fetchable and a
        # missed day repairs itself on the next poll rather than needing a backfill.
        backfill_horizon=BackfillHorizon.COMPLETE,
        # 48 h — two consecutive missed runs. This is the 2x floor
        # `test_freshness_sla_is_longer_than_the_poll_cadence` enforces, not the 3x the
        # other sources use, and the difference is what a multiple *means* at this cadence:
        # 3x a 15-minute poll is 45 minutes, while 3x a daily poll is three days of silence
        # before anyone hears about it. One missed run on a daily schedule is a real event
        # and 30 h would have caught it — but it would also fire on any run that merely
        # started late, which is how an alert gets trained away (SPEC §11). Two misses is
        # the first unambiguous signal.
        freshness_sla_seconds=172800,
        # Zero, like `rss_ars` and for a sharper version of the same reason: health is
        # assessed over the closed prior *hour* (`monitor.window_bounds`), and a daily
        # source is legitimately silent in 23 of them. Any positive floor would report this
        # source as thin almost permanently. What catches a genuinely dead market feed is
        # the content-staleness SLA below, which is measured against `last_content_change_at`
        # rather than volume (SPEC §11, 1.E).
        min_docs_per_window=0,
        # 120 h, past the longest legitimate gap. Markets close weekends, and a Thursday-
        # Friday holiday puts ~4 days between sessions — the same reasoning as `edgar`'s
        # 96 h, extended because equities add no partial-day filings in between. Firing
        # every Thanksgiving is how an alert gets trained away (SPEC §11).
        content_staleness_sla_seconds=432000,
        rate_limit_per_sec=2.0,
        timeout_seconds=15.0,
        user_agent=settings.user_agent,
    ),
}

# The deployed sources: everything with a Lambda, a schedule, and a state item. `fake` is
# the Phase 0 fixture source and has none of those, so assessing it would report a
# permanent outage for something that was never running. Derived rather than listed,
# because a hardcoded copy in the DAG is exactly how SPEC §3's 30-minute claim quietly
# stops being true.
DEPLOYED_SOURCE_IDS: tuple[str, ...] = tuple(s for s in SOURCES if s != "fake")
