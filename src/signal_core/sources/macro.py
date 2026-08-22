"""ALFRED macro series, every vintage. SPEC §8; docs/runbooks/phase-4b.md 4B.H.

FRED serves the *current* value of a series. **ALFRED serves every value the series has ever
had**, and that is the entire reason this source exists: SPEC §8's argument is that a normal
pipeline overwrites a revision and quietly destroys the record, while this one keeps both time
axes so "what was knowable on 2026-03-14" is a query rather than an archaeology project.

Mechanically, ALFRED is the same API with a real-time range. Ask for
`realtime_start=1776-07-04&realtime_end=9999-12-31` and each observation comes back once per
interval during which it was the current value:

    {"date": "2026-06-01", "realtime_start": "2026-07-02", "realtime_end": "2026-08-06",
     "value": "159842"}

`date` is the period the number describes (**valid time**) and `realtime_start` is the day it
became the published figure (**known time**). A row whose `realtime_end` is not the far-future
sentinel has been superseded — which is to say, revised.

## The key

FRED requires one, which was confirmed against the live endpoint before this module was
written rather than assumed: an unauthenticated request answers HTTP 400 with
`Variable api_key is not set`. This is the first source in the project to need a secret, and
it reaches the Lambda through SSM Parameter Store rather than an environment variable — see
`infra/terraform/main/macro.tf` for why.

## The window is bounded, and the bound is a decision

`REALTIME_START` is 2015-01-01, not the beginning of each series. §8's worked payoff is
"payrolls revised down 46k across the prior two months" — a claim about recent revisions —
and a full vintage history of PAYEMS back to 1939 is a much larger response for data no brief
will ever cite. It also keeps every request well inside FRED's 100,000-observation ceiling,
which a full daily series across all vintages could plausibly approach. Widening it is a
one-line change and costs only bytes.

## Every fetch re-states everything

The full bounded history is re-fetched on every poll rather than walking a watermark forward.
That is deliberate, and it is the same argument `market.py` makes for its `3mo` range: a
missed day repairs itself on the next poll instead of needing a backfill, and replay (SPEC
§6.3) becomes trivially correct because no fetch depends on the previous one having happened.
A published vintage never changes, so re-reading it is free of consequence — the MERGE in
`spark/jobs/macro.py` collapses it to nothing.
"""

from __future__ import annotations

import time
from datetime import datetime

import httpx

from signal_core.contracts import FetchOutcome, RawDocument, SourceConfig, State
from signal_core.hashing import content_hash
from signal_core.timeutil import utc_now
from signal_core.watchlist import load as load_watchlist

# See the module docstring. The far-future end is ALFRED's own sentinel for "still current".
REALTIME_START = "2015-01-01"
REALTIME_END = "9999-12-31"

# FRED asks for no specific rate but the courtesy pacing every other poller here uses applies:
# six sequential requests at this spacing is a few seconds.
REQUEST_SPACING_SECONDS = 0.5

# The ALFRED observations endpoint.
PATH = "/fred/series/observations"

# What Terraform writes into the SSM parameter it creates. It owns the parameter's existence
# and its IAM scoping; it never owns the secret. Matched here so the poller can say precisely
# which of "no parameter", "no value" and "not set yet" it is looking at.
PLACEHOLDER = "UNSET"


# Resolved once per container rather than once per invocation. A Lambda that polls six series
# every morning would otherwise pay an SSM call and a KMS decrypt on every warm start for a
# value that does not change. Reset by a cold start, which is also how a rotated key takes
# effect — stated because "why did the old key keep working for ten minutes" is the question
# this cache will eventually raise.
_API_KEY_CACHE: dict[str, str] = {}


def _api_key(config: SourceConfig) -> str:
    """The FRED key, from `options` in tests and from SSM Parameter Store in the Lambda.

    Read here rather than passed in through the environment, and the difference is the point:
    an environment variable would put the key in Terraform state (which lives in S3) and in
    the Lambda console. See `infra/terraform/main/macro.tf`.
    """
    direct = config.options.get("api_key")
    if direct:
        return str(direct)

    parameter = config.options.get("api_key_parameter")
    if not parameter:
        raise LookupError(
            "no FRED api_key and no api_key_parameter in SourceConfig.options — "
            "see infra/terraform/main/macro.tf"
        )
    if parameter in _API_KEY_CACHE:
        return _API_KEY_CACHE[parameter]

    # boto3 is imported lazily, matching `staging._s3` and `ops/athena._athena_client`:
    # local callers and every test that injects a key never need it.
    import boto3

    try:
        response = boto3.client("ssm").get_parameter(Name=parameter, WithDecryption=True)
        value = response["Parameter"]["Value"]
    except Exception as exc:
        # Deliberately broad. A missing parameter, a denied decrypt and an SSM outage are
        # three different AWS exception types and one fact to the caller: the key could not be
        # read, so this poll is an ERROR document rather than a crash.
        raise LookupError(
            f"could not read {parameter} from SSM: {type(exc).__name__}: {exc}"
        ) from exc

    if not value or value == PLACEHOLDER:
        # Terraform creates the parameter with this placeholder and never owns its value, so
        # this is the expected state between `terraform apply` and the manual put-parameter.
        # Saying so precisely is the difference between a five-second fix and a debugging
        # session — the same courtesy `mail.tf` extends for its unverified SES identity.
        raise LookupError(
            f"{parameter} still holds the Terraform placeholder — set the real key with "
            f"`aws ssm put-parameter --name {parameter} --type SecureString "
            "--value <key> --overwrite`"
        )
    _API_KEY_CACHE[parameter] = value
    return value


def poll(config: SourceConfig, state: State) -> tuple[list[RawDocument], State]:
    tickers = sorted(load_watchlist().macro_series)
    if not tickers:
        # An empty `macro_series` is a configuration statement, not a failure — the same
        # reading `market.py` gives an empty ticker list. Reporting it as an error would page
        # someone about a file they meant to empty.
        now = utc_now()
        return [], state.model_copy(update={"last_success_at": now, "consecutive_failures": 0})

    try:
        api_key = _api_key(config)
    except LookupError as exc:
        # An ERROR document, never an escaped exception: a missing or unreadable key is a
        # configuration fault the monitoring layer should surface as a degraded source, not an
        # infrastructure crash the CloudWatch alarms page about (SPEC §6.1).
        return _error_batch(config, tickers, str(exc))

    documents: list[RawDocument] = []
    with httpx.Client(
        base_url=config.url,
        timeout=config.timeout_seconds,
        headers={"User-Agent": config.user_agent},
    ) as client:
        for index, series_id in enumerate(tickers):
            if index:
                time.sleep(REQUEST_SPACING_SECONDS)
            documents.append(_fetch_series(client, config, series_id, api_key))

    now = utc_now()
    ok = [d for d in documents if d.outcome is FetchOutcome.OK]
    # Hashed collectively, the way `market.py` hashes its tickers and for the same reason: the
    # honest question for this source is "did any vintage anywhere move", not "did PAYEMS
    # move". Sorted so the digest does not depend on request ordering. A per-series hash would
    # need somewhere per-series to live, and `State` holds one.
    combined = content_hash(b"".join(sorted(d.content_hash.encode() for d in ok))) if ok else None
    changed = bool(combined and combined != state.last_content_hash)

    return documents, state.model_copy(
        update={
            "last_success_at": now if ok else state.last_success_at,
            "consecutive_failures": 0 if ok else state.consecutive_failures + 1,
            "last_content_hash": combined or state.last_content_hash,
            "last_content_change_at": now if changed else state.last_content_change_at,
        }
    )


def _fetch_series(
    client: httpx.Client, config: SourceConfig, series_id: str, api_key: str
) -> RawDocument:
    """One series, all its vintages inside the bounded window.

    The poller fetches and reports; it does not interpret (SPEC §6.1). Even a `count` that
    looks truncated becomes a warning on an otherwise-OK document rather than a decision here,
    because deciding what a short series means is interpretation and belongs in Spark, against
    stored bytes, where a mistake is fixable.
    """
    now = utc_now()
    started = time.monotonic()
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "realtime_start": REALTIME_START,
        "realtime_end": REALTIME_END,
    }
    try:
        response = client.get(PATH, params=params)
        response.raise_for_status()
        payload = response.content
        http_status = response.status_code
        outcome = FetchOutcome.OK
    except httpx.HTTPStatusError as exc:
        # FRED puts a readable reason in the body on a 400 — a bad key, an unknown series id.
        # Storing that body as the payload is what makes the failure diagnosable straight out
        # of bronze without re-running anything, and it is the same shape `market.py` uses.
        payload = exc.response.content or str(exc).encode("utf-8")
        http_status = exc.response.status_code
        outcome = FetchOutcome.ERROR
    except httpx.HTTPError as exc:
        payload = str(exc).encode("utf-8")
        http_status = 0
        outcome = FetchOutcome.ERROR

    return _document(config, series_id, now, started, payload, http_status, outcome)


def _document(
    config: SourceConfig,
    series_id: str,
    now: datetime,
    started: float,
    payload: bytes,
    http_status: int,
    outcome: FetchOutcome,
) -> RawDocument:
    """Build the bronze row.

    **The API key is never in `source_url`.** The URL is reconstructed from the parameters
    that matter rather than taken from `response.request.url`, because bronze is immutable and
    a secret written into it cannot be redacted later — only the whole object deleted. This is
    also the field `spark/jobs/macro.py::series_id_from_url` reads the series id back out of,
    so it has to carry `series_id` and nothing sensitive.
    """
    source_url = (
        f"{config.url}{PATH}?series_id={series_id}"
        f"&realtime_start={REALTIME_START}&realtime_end={REALTIME_END}&file_type=json"
    )
    return RawDocument(
        ingest_id=f"{config.source_id}-{now:%Y%m%dT%H%M%S%f}-{series_id}",
        source_id=config.source_id,
        fetched_at=now,
        source_url=source_url,
        http_status=http_status,
        outcome=outcome,
        etag=None,
        last_modified=None,
        content_hash=content_hash(payload),
        payload=payload,
        payload_format=config.payload_format,
        latency_ms=int((time.monotonic() - started) * 1000),
        byte_count=len(payload),
    )


def _error_batch(
    config: SourceConfig, series_ids: list[str], error: str
) -> tuple[list[RawDocument], State]:
    """One ERROR document per series when the key itself could not be resolved.

    Per series rather than one for the batch, so `ops.source_health` sees the same document
    count shape it sees on a good run — a single row would read as "the source produced one
    document today", which is a different and less alarming fact than "all six failed".
    """
    now = utc_now()
    started = time.monotonic()
    payload = error.encode("utf-8")
    documents = [
        _document(config, series_id, now, started, payload, 0, FetchOutcome.ERROR)
        for series_id in series_ids
    ]
    return documents, State(source_id=config.source_id, consecutive_failures=1)
