-- ops.maintenance_runs — compaction and expiry, per table per night. SPEC §12.
--
-- Import target: "Maintenance". Grain (run_id, table_name).
--
-- `error` and `skipped` are the two columns worth a visual: a maintenance run that
-- skipped a table degrades quietly by design (spark/jobs/maintain.py) rather than failing
-- the DAG, so a skip streak is visible here and nowhere else.
SELECT
    run_date,
    table_name,
    run_id,
    files_before,
    files_after,
    rewritten_files,
    added_files,
    rewritten_bytes,
    deleted_manifests,
    orphans_removed,
    error,
    skipped
FROM ops.maintenance_runs
-- months(run_date) is the partition.
WHERE run_date >= date_add('day', -90, current_date)
