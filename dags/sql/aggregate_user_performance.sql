INSERT INTO filter.time_windows (window_start)
VALUES (date_trunc('minute', NOW()) - interval '5 minutes')
ON CONFLICT (window_start) DO NOTHING;

WITH window_bounds AS (
	SELECT
		date_trunc('minute', NOW()) - interval '5 minutes' AS window_start,
		date_trunc('minute', NOW()) AS window_end
)

INSERT INTO analytics.user_performance (
    window_start,
	user_id,
    country,
    region,
    account_tier,
    device_type,
	age_group,
	gender,
	signup_date,
    purchases,
    total_quantity,
    revenue,
	avg_order_value,
	session_count
)

SELECT
    w.window_start,
	e.user_id,
    u.country,
    u.region,
    u.account_tier,
    u.device_type,
    u.age_group,
    u.gender,
    u.signup_date,
    COUNT(*) AS purchases,
    SUM(e.quantity) AS total_quantity,
    SUM(e.price_total) AS revenue,
	ROUND(
		SUM(e.price_total)
		/ NULLIF(COUNT(*) FILTER (WHERE e.price_total IS NOT NULL), 0),
	2) AS avg_order_value,
	COUNT(DISTINCT e.session_id) AS session_count

FROM raw.events e
CROSS JOIN window_bounds w
JOIN dim.users u ON e.user_id = u.user_id

WHERE e.event_type = 'purchase'
	AND e.ingested_at >= w.window_start
	AND e.ingested_at < w.window_end

GROUP BY w.window_start, e.user_id, u.country, u.region, u.account_tier, u.device_type, u.age_group, u.gender, u.signup_date

ON CONFLICT (window_start, user_id)
DO UPDATE SET
    purchases = EXCLUDED.purchases,
    total_quantity = EXCLUDED.total_quantity,
    revenue = EXCLUDED.revenue,
    avg_order_value = EXCLUDED.avg_order_value,
    session_count = EXCLUDED.session_count;