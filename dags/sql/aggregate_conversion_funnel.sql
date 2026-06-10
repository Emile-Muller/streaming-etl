INSERT INTO filter.time_windows (window_start)
VALUES (date_trunc('minute', NOW()) - interval '5 minutes')
ON CONFLICT (window_start) DO NOTHING;

WITH window_bounds AS (
	SELECT
		date_trunc('minute', NOW()) - interval '5 minutes' AS window_start,
		date_trunc('minute', NOW()) AS window_end
),

session_events AS (
	SELECT
		e.*,
		MIN(timestamp) FILTER (WHERE e.event_type = 'view_product') OVER (PARTITION BY e.session_id) AS first_view,
		MIN(timestamp) FILTER (WHERE e.event_type = 'add_to_cart')  OVER (PARTITION BY e.session_id) AS first_cart
	FROM raw.events e, window_bounds w
	WHERE e.event_type IN ('view_product', 'add_to_cart', 'purchase', 'remove_from_cart')
		AND e.ingested_at >= w.window_start
		AND e.ingested_at < w.window_end
),

session_timings AS (
	SELECT
		session_id,
		product_id,
		MIN(timestamp) FILTER (WHERE event_type = 'view_product') AS viewed_at,
		MIN(timestamp) FILTER (WHERE event_type = 'add_to_cart'
			AND timestamp >= first_view) AS carted_at,
		MIN(timestamp) FILTER (WHERE event_type = 'purchase'
			AND timestamp >= first_cart) AS purchased_at,
		MIN(timestamp) FILTER (WHERE event_type = 'remove_from_cart'
			AND timestamp >= first_cart) AS removed_at
	FROM session_events
	GROUP BY session_id, product_id
),

session_rates AS (
	SELECT
		ROUND(
			COUNT(*) FILTER (WHERE carted_at IS NOT NULL)::NUMERIC
			/ NULLIF(COUNT(*) FILTER (WHERE viewed_at IS NOT NULL), 0), 4
		) AS avg_view_to_cart_rate,
		ROUND(
			COUNT(*) FILTER (WHERE purchased_at IS NOT NULL)::NUMERIC
			/ NULLIF(COUNT(*) FILTER (WHERE carted_at IS NOT NULL), 0), 4
		) AS avg_cart_to_purchase_rate,
		ROUND(
			COUNT(*) FILTER (WHERE purchased_at IS NOT NULL)::NUMERIC
			/ NULLIF(COUNT(*) FILTER (WHERE viewed_at IS NOT NULL), 0), 4
		) AS avg_view_to_purchase_rate,
		ROUND(
			COUNT(*) FILTER (WHERE removed_at IS NOT NULL)::NUMERIC
			/ NULLIF(COUNT(*) FILTER (WHERE carted_at IS NOT NULL), 0), 4
		) AS avg_cart_to_delete_rate
	FROM session_timings
)

INSERT INTO analytics.conversion_funnel (
	window_start,
	avg_view_to_cart_sec,
	avg_cart_to_purchase_sec,
	avg_view_to_purchase_sec,
	avg_cart_to_delete_sec,
	avg_view_to_cart_rate,
	avg_cart_to_purchase_rate,
	avg_view_to_purchase_rate,
	avg_cart_to_delete_rate
)
SELECT
	w.window_start,
	ROUND(AVG(
		EXTRACT(EPOCH FROM (t.carted_at - t.viewed_at))
	) FILTER (WHERE t.carted_at IS NOT NULL AND t.viewed_at IS NOT NULL), 2) AS avg_view_to_cart_sec,
	ROUND(AVG(
		EXTRACT(EPOCH FROM (t.purchased_at - t.carted_at))
	) FILTER (WHERE t.purchased_at IS NOT NULL AND t.carted_at IS NOT NULL), 2) AS avg_cart_to_purchase_sec,
	ROUND(AVG(
		EXTRACT(EPOCH FROM (t.purchased_at - t.viewed_at))
	) FILTER (WHERE t.purchased_at IS NOT NULL AND t.viewed_at IS NOT NULL), 2) AS avg_view_to_purchase_sec,
	ROUND(AVG(
		EXTRACT(EPOCH FROM (t.removed_at - t.carted_at))
	) FILTER (WHERE t.removed_at IS NOT NULL AND t.carted_at IS NOT NULL), 2) AS avg_cart_to_delete_sec,
	r.avg_view_to_cart_rate,
	r.avg_cart_to_purchase_rate,
	r.avg_view_to_purchase_rate,
	r.avg_cart_to_delete_rate

FROM session_timings t
CROSS JOIN window_bounds w
CROSS JOIN session_rates r
GROUP BY w.window_start,
	r.avg_view_to_cart_rate,
	r.avg_cart_to_purchase_rate,
	r.avg_view_to_purchase_rate,
	r.avg_cart_to_delete_rate

ON CONFLICT (window_start)
DO UPDATE SET
	avg_view_to_cart_sec = EXCLUDED.avg_view_to_cart_sec,
	avg_cart_to_purchase_sec = EXCLUDED.avg_cart_to_purchase_sec,
	avg_view_to_purchase_sec = EXCLUDED.avg_view_to_purchase_sec,
	avg_cart_to_delete_sec = EXCLUDED.avg_cart_to_delete_sec,
	avg_view_to_cart_rate = EXCLUDED.avg_view_to_cart_rate,
	avg_cart_to_purchase_rate = EXCLUDED.avg_cart_to_purchase_rate,
	avg_view_to_purchase_rate = EXCLUDED.avg_view_to_purchase_rate,
	avg_cart_to_delete_rate = EXCLUDED.avg_cart_to_delete_rate;