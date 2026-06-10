INSERT INTO filter.time_windows (window_start)
VALUES (date_trunc('minute', NOW()) - interval '5 minutes')
ON CONFLICT (window_start) DO NOTHING;

WITH window_bounds AS (
	SELECT
		date_trunc('minute', NOW()) - interval '5 minutes' AS window_start,
		date_trunc('minute', NOW()) AS window_end
)

INSERT INTO analytics.overall_performance (
	window_start,
	views,
	add_to_carts,
	remove_from_carts,
	purchases,
	revenue,
	avg_order_value
)

SELECT
	w.window_start,
	COUNT(*) FILTER (WHERE event_type = 'view_product') AS views,
	COUNT(*) FILTER (WHERE event_type = 'add_to_cart') AS add_to_carts,
	COUNT(*) FILTER (WHERE event_type = 'remove_from_cart') AS remove_from_carts,
	COUNT(*) FILTER (WHERE event_type = 'purchase') AS purchases,
	SUM(price_total) FILTER (WHERE event_type = 'purchase') AS revenue,
	ROUND(
		SUM(price_total) FILTER (WHERE event_type = 'purchase')
		/ NULLIF(COUNT(*) FILTER (WHERE event_type = 'purchase' AND price_total IS NOT NULL), 0),
	2) AS avg_order_value

FROM raw.events e
CROSS JOIN window_bounds w

WHERE e.ingested_at >= w.window_start
	AND e.ingested_at < w.window_end
	
GROUP BY w.window_start

ON CONFLICT (window_start)
DO UPDATE SET
	views = EXCLUDED.views,
	add_to_carts = EXCLUDED.add_to_carts,
	remove_from_carts = EXCLUDED.remove_from_carts,
	purchases = EXCLUDED.purchases,
	revenue = EXCLUDED.revenue,
	avg_order_value = EXCLUDED.avg_order_value;