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

    @property
    def user_agent(self) -> str:
        """SEC requires a descriptive User-Agent carrying a contact email. SPEC §6.2."""
        return f"signal/0.0 (+https://github.com/signal; {self.contact_email})"

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
    def warehouse_uri(self) -> str:
        """Iceberg warehouse root. Defaults to a local directory so a fresh clone can run
        the commit job and the Phase 1 acceptance test without an AWS account."""
        return self.iceberg_warehouse or str(self.data_root / "warehouse")

    @property
    def silver_root(self) -> Path:
        return self.data_root / "silver"


settings = Settings()

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
        freshness_sla_seconds=60,
        min_docs_per_window=0,  # a quiet minute on HN is normal, not a failure
        rate_limit_per_sec=5.0,
        user_agent=settings.user_agent,
    ),
    "edgar": SourceConfig(
        source_id="edgar",
        url="https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&output=atom",
        payload_format=PayloadFormat.XML,
        # Current feed only recovers ~1 day; the daily index fallback is Phase 2+. SPEC §3.
        backfill_horizon=BackfillHorizon.DAY,
        freshness_sla_seconds=900,
        min_docs_per_window=1,
        rate_limit_per_sec=1.0,  # SEC fair-access limits; a descriptive User-Agent is required
        user_agent=settings.user_agent,
    ),
    "rss_tech": SourceConfig(
        source_id="rss_tech",
        url="https://techcrunch.com/feed/",
        payload_format=PayloadFormat.XML,
        # Only what is still in the feed survives an outage. SPEC §3, §6.3.
        backfill_horizon=BackfillHorizon.WINDOW,
        freshness_sla_seconds=1800,
        min_docs_per_window=1,
        rate_limit_per_sec=1.0,
        user_agent=settings.user_agent,
    ),
}
