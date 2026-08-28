-- bronze.raw_documents, aggregated. The fetch-level view of ingestion state.
--
-- Import target: "Ingest outcomes". Grain (ingest_date, source_id, outcome).
--
-- **Aggregated in Athena on purpose — never import this table row by row.** Every row
-- carries a `payload binary` column holding the raw fetched bytes; bronze is the
-- immutable record, not a BI source. A `SELECT *` here is precisely what the workgroup's
-- 100 MB bytes_scanned_cutoff_per_query exists to stop, and it would stop it by failing
-- your refresh rather than by warning you.
--
-- The three outcomes are distinct facts and must stay distinct in any visual:
--   ok           — fetched something
--   not_modified — healthy 304, the source had nothing new and said so
--   empty        — 200, nothing new
--   error        — the fetch failed (recorded as a document, never an escaped exception)
-- Collapsing not_modified and empty into "0 docs" hides the stale-but-successful failure
-- mode the whole monitoring layer exists to catch.
SELECT
    ingest_date,
    source_id,
    outcome,
    count(*)             AS documents,
    sum(byte_count)      AS bytes_fetched,
    approx_percentile(latency_ms, 0.5)  AS p50_latency_ms,
    approx_percentile(latency_ms, 0.95) AS p95_latency_ms
FROM bronze.raw_documents
-- (source_id, ingest_date) is the partition spec, so this prunes on both.
WHERE ingest_date >= date_add('day', -90, current_date)
GROUP BY ingest_date, source_id, outcome
