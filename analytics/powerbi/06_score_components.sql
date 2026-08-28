-- gold.brief_items, one row per score component. Why an item ranked where it did.
--
-- Import target: "Score components". Grain (brief_date, rank, component).
--
-- The map is stored rather than the weighted total per component precisely so the
-- explanation survives a weight change (brief/items.py) — which means the honest chart is
-- component value over time, not a single ranking factor treated as constant.
SELECT
    b.brief_date,
    b.rank,
    b.cluster_id,
    c.component,
    c.value
FROM gold.brief_items AS b
CROSS JOIN UNNEST(b.score_components) AS c (component, value)
WHERE b.brief_date >= date_add('day', -90, current_date)
