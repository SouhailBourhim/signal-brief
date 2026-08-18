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

    @property
    def user_agent(self) -> str:
        """SEC requires a descriptive User-Agent carrying a contact email. SPEC §6.2."""
        return f"signal/0.0 (+https://github.com/signal; {self.contact_email})"

    @property
    def bronze_root(self) -> Path:
        return self.data_root / "bronze"

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
    # Phase 1 — registered here so the shape is visible; modules land with Phase 1.
    # "hackernews": ... BackfillHorizon.COMPLETE, 15 min SLA
    # "edgar":      ... BackfillHorizon.DAY,      near-real-time SLA
    # "rss_tech":   ... BackfillHorizon.WINDOW,   30 min SLA
}
