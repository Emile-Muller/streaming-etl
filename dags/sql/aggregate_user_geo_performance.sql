INSERT INTO filter.time_windows (window_start)
VALUES (date_trunc('minute', NOW()) - interval '5 minutes')
ON CONFLICT (window_start) DO NOTHING;

WITH window_bounds AS (
	SELECT
		date_trunc('minute', NOW()) - interval '5 minutes' AS window_start,
		date_trunc('minute', NOW()) AS window_end
)

INSERT INTO analytics.user_geo_performance (
	window_start,
	country,
	region,
	purchases,
	revenue,
	avg_order_value
)

SELECT
    w.window_start,
    u.country,
    u.region,
    COUNT(*) AS purchases,
    SUM(e.price_total) AS revenue,
	ROUND(
		SUM(e.price_total)
		/ NULLIF(COUNT(*) FILTER (WHERE e.price_total IS NOT NULL), 0),
	2) AS avg_order_value

FROM raw.events e
CROSS JOIN window_bounds w
JOIN dim.users u ON e.user_id = u.user_id

WHERE e.event_type = 'purchase'
	AND e.ingested_at >= w.window_start
	AND e.ingested_at < w.window_end

GROUP BY w.window_start, u.country, u.region

ON CONFLICT (window_start, country, region)
DO UPDATE SET
    purchases = EXCLUDED.purchases,
    revenue = EXCLUDED.revenue,
    avg_order_value = EXCLUDED.avg_order_value;