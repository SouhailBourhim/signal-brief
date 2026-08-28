-- ops.source_health — one row per source per monitoring window. The state table.
--
-- Import target: "Source health". Grain (source_id, window_start) matches the MERGE key
-- in spark/jobs/health_snapshot.py, so this is already deduplicated at the source.
--
-- `staleness_seconds` is NULL where the source never succeeded (inf is unrepresentable in
-- a double column anyone will later average — `status` already carries that fact), and
-- `content_staleness_seconds` is NULL where a source has no content-movement signal at
-- all. Neither NULL means "fresh"; in Power BI, leave them blank rather than coalescing
-- to zero, or a dead feed reads as a healthy one.
SELECT
    source_id,
    window_start,
    status,
    gap_reason,
    docs_ingested,
    expected_min,
    baseline_docs,
    last_success_at,
    staleness_seconds,
    content_staleness_seconds
FROM ops.source_health
-- months(window_start) is the partition. Filtering on the partition column is what makes
-- this cheap; see the measurement in docs/athena.md.
WHERE window_start >= date_add('day', -90, current_timestamp)
