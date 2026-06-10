INSERT INTO filter.time_windows (window_start)
VALUES (date_trunc('minute', NOW()) - interval '5 minutes')
ON CONFLICT (window_start) DO NOTHING;

WITH window_bounds AS (
	SELECT
		date_trunc('minute', NOW()) - interval '5 minutes' AS window_start,
		date_trunc('minute', NOW()) AS window_end
),

events_bucketed AS (
    SELECT
        e.*,
		w.window_start,
        CASE
            WHEN e.discount_pct IS NULL THEN 'unknown'
            WHEN e.discount_pct = 0 THEN 'no_discount'
            WHEN e.discount_pct < 10 THEN '0-10%'
            WHEN e.discount_pct < 25 THEN '10-25%'
            WHEN e.discount_pct < 50 THEN '25-50%'
            ELSE '50%+'
        END AS discount_bucket
    FROM raw.events e
	CROSS JOIN window_bounds w
    WHERE e.event_type = 'purchase'
        AND e.ingested_at >= w.window_start
        AND e.ingested_at < w.window_end
)

INSERT INTO analytics.discount_sales (
	window_start,
	discount_bucket,
	purchases,
	total_quantity,
	revenue,
	avg_discount_pct
)

SELECT
	window_start,
	discount_bucket,
	COUNT(*) AS purchases,
	SUM(quantity) AS total_quantity,
	SUM(price_total) AS revenue,
	ROUND(AVG(discount_pct), 2) AS avg_discount_pct

FROM events_bucketed
	
GROUP BY window_start, discount_bucket

ON CONFLICT (window_start, discount_bucket)
DO UPDATE SET
	purchases = EXCLUDED.purchases,
	total_quantity = EXCLUDED.total_quantity,
	revenue = EXCLUDED.revenue,
	avg_discount_pct = EXCLUDED.avg_discount_pct;