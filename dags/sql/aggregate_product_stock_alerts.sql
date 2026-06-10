INSERT INTO filter.time_windows (window_start)
VALUES (date_trunc('minute', NOW()) - interval '5 minutes')
ON CONFLICT (window_start) DO NOTHING;

WITH window_bounds AS (
	SELECT
		date_trunc('minute', NOW()) - interval '5 minutes' AS window_start,
		date_trunc('minute', NOW()) AS window_end
)

INSERT INTO analytics.product_stock_alerts (
	window_start,
	product_id,
	name,
	brand,
	category,
	marketplace,
	availability,
	views
)

SELECT
	w.window_start,
	p.product_id,
	p.name,
	p.brand,
	p.category,
	p.marketplace,
	p.availability,
	COUNT(e.event_id) AS views
FROM dim.products p
CROSS JOIN window_bounds w
LEFT JOIN raw.events e ON e.product_id = p.product_id
	AND e.event_type = 'view_product'
	AND e.ingested_at >= w.window_start
	AND e.ingested_at < w.window_end
	
WHERE p.availability IN ('out_of_stock', 'limited')

GROUP BY w.window_start, p.product_id, p.name, p.brand, p.category, p.marketplace, p.availability

ON CONFLICT (window_start, product_id)
DO UPDATE SET
	name = EXCLUDED.name,
	brand = EXCLUDED.brand,
	category = EXCLUDED.category,
	marketplace = EXCLUDED.marketplace,
	availability = EXCLUDED.availability,
	views = EXCLUDED.views;