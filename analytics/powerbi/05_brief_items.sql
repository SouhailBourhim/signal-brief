-- gold.brief_items — what the reader was actually shown, and what they said about it.
--
-- Import target: "Brief items". Grain (brief_date, rank).
--
-- Only clusters that were *shown* are recorded (brief/items.py), so this is the published
-- brief, not the candidate pool. `user_feedback` is NULL until someone marks an item —
-- that is "no opinion yet", not a negative one.
--
-- `score_components` is a map<string, double> and the ODBC driver has no map type; it
-- arrives as an opaque string or an error. Cast it to JSON here so the column at least
-- survives the trip, and use 06_score_components.sql for anything that charts it.
SELECT
    brief_date,
    rank,
    cluster_id,
    title,
    score,
    included,
    user_feedback,
    created_at,
    CAST(score_components AS JSON) AS score_components_json
FROM gold.brief_items
-- Unpartitioned (created through Athena, not Spark), so this filter cuts rows, not files.
WHERE brief_date >= date_add('day', -90, current_date)
