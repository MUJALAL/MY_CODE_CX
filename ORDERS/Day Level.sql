SELECT
    Day,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT order_id) FILTER(WHERE status = 4) AS completed_orders,
    COUNT(DISTINCT order_id) FILTER(WHERE status = 5) AS cancelled_orders
FROM
(
    SELECT 
        id, 
        order_id, 
        status, 
        DATE(to_timestamp(pickup_time + 19800)) AS Day
    FROM 
        orders 
    WHERE 
        order_type = 0 
    AND 
        deleted_at IS NULL 
    AND 
        status IN (4,5) 
    AND
        pickup_time >= EXTRACT(EPOCH FROM CURRENT_DATE::TIMESTAMP - INTERVAL '5 Days') - 19800
    AND
        pickup_time < EXTRACT (EPOCH FROM CURRENT_DATE::TIMESTAMP) - 19800
)five_days

GROUP BY Day;




SELECT CURRENT_DATE:: TIMESTAMP, CURRENT_DATE:: TIMESTAMP - interval '5 hours 30 minutes', EXTRACT (EPOCH FROM CURRENT_DATE::TIMESTAMP), CURRENT_DATE:: TIMESTAMP - interval '5 days', CURRENT_DATE:: TIMESTAMP - interval '5 days' - interval '5 hours 30 minutes', EXTRACT(EPOCH FROM CURRENT_DATE::TIMESTAMP - INTERVAL '5 Days')
