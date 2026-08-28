-- ops.pipeline_costs — what each DAG task actually spent. SPEC §10.3, §17.
--
-- Import target: "Pipeline costs". Grain (run_id, dag_id, task_id).
--
-- Every numeric column is nullable on purpose: one task measures Athena bytes scanned,
-- another measures S3 egress, and neither fabricates the field it did not measure. A
-- Power BI SUM() ignores NULLs, which is the correct reading — but a *count* of rows is
-- not a count of measurements, so build measures over the specific column, not over rows.
--
-- `athena_cost_usd` is floored at Athena's 10 MB per-query minimum by
-- ops/athena.py::athena_cost_usd. Summing it across many tiny queries gives a figure that
-- matches the bill; deriving your own dollars from `bytes_scanned` in DAX would not.
SELECT
    run_date,
    dag_id,
    task_id,
    run_id,
    bytes_scanned,
    athena_cost_usd,
    lambda_ms,
    s3_requests,
    s3_egress_bytes
FROM ops.pipeline_costs
-- months(run_date) is the partition.
WHERE run_date >= date_add('day', -90, current_date)
