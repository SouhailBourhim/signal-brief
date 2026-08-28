-- gold.cluster_enrichment and gold.enrichment_rejects — the LLM stage's state.
--
-- Import target: "Enrichment". Grain (day, model_digest, prompt_version, status).
--
-- Accepted and rejected enrichments live in two tables because a reject carries the raw
-- model output for a person to read, and the accepted row does not. Unioned here into one
-- narrow fact so a single visual can show the accept/reject split per model version --
-- `model_digest` and `prompt_version` are what make a change in that ratio attributable.
--
-- `cache_hit` matters: a cache hit spent no inference. Counting hits as work done would
-- overstate what the model actually produced.
SELECT
    date(generated_at) AS day,
    model_digest,
    prompt_version,
    'accepted'         AS status,
    count(*)           AS clusters,
    count_if(cache_hit) AS cache_hits
FROM gold.cluster_enrichment
WHERE generated_at >= date_add('day', -90, current_timestamp)
GROUP BY date(generated_at), model_digest, prompt_version

UNION ALL

SELECT
    date(rejected_at) AS day,
    model_digest,
    prompt_version,
    'rejected'        AS status,
    count(*)          AS clusters,
    CAST(NULL AS bigint) AS cache_hits
FROM gold.enrichment_rejects
WHERE rejected_at >= date_add('day', -90, current_timestamp)
GROUP BY date(rejected_at), model_digest, prompt_version
