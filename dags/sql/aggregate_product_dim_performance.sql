INSERT INTO filter.time_windows (window_start)
VALUES (date_trunc('minute', NOW()) - interval '5 minutes')
ON CONFLICT (window_start) DO NOTHING;

WITH window_bounds AS (
	SELECT
		date_trunc('minute', NOW()) - interval '5 minutes' AS window_start,
		date_trunc('minute', NOW()) AS window_end
)

INSERT INTO analytics.product_dim_performance (
	window_start,
	brand,
	category,
	marketplace,
	purchases,
	total_quantity,
	revenue,
	avg_order_value
)
SELECT
	w.window_start,
	p.brand,
	p.category,
	p.marketplace,
	COUNT(*) AS purchases,
	SUM(e.quantity) AS total_quantity,
	SUM(e.price_total) AS revenue,
	ROUND(
		SUM(e.price_total)
		/ NULLIF(COUNT(*) FILTER (WHERE e.price_total IS NOT NULL), 0),
	2) AS avg_order_value
	
FROM raw.events e
CROSS JOIN window_bounds w
JOIN dim.products p ON e.product_id = p.product_id

WHERE e.event_type = 'purchase'
	AND e.ingested_at >= w.window_start
	AND e.ingested_at < w.window_end
	
GROUP BY w.window_start, p.brand, p.category, p.marketplace

ON CONFLICT (window_start, brand, category, marketplace)
DO UPDATE SET
	purchases = EXCLUDED.purchases,
	total_quantity = EXCLUDED.total_quantity,
	revenue = EXCLUDED.revenue,
	avg_order_value = EXCLUDED.avg_order_value;