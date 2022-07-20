--1. LAST FIVE DAYS DATA

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


2. Per Region

SELECT
		Day,
		reg,
		gname,
		count(DISTINCT order_id) as Total_Order,
		count(DISTINCT order_id) filter(WHERE status = 4) as Completed_Order,
		count(DISTINCT order_id) filter(WHERE status = 5) as Cancelled_Order
FROM
(	SELECT 
		ord.id, 
		ord.order_id,
		ord.geo_region_id as reg,
		geo.name as gname,
		ord.status,
		DATE(to_timestamp(pickup_time + 19800)) as Day
	
	FROM 
		orders as ord
	
	JOIN 
		geo_regions as geo
	
	ON 	ord.geo_region_id = geo.id
	
	WHERE
		order_type = 0
	AND
		deleted_at is NULL
	AND
		ord.status in (4,5)
	AND
		pickup_time >= EXTRACT(EPOCH FROM CURRENT_DATE::TIMESTAMP - INTERVAL '5 Days') - 19800
	AND
		pickup_time < EXTRACT(EPOCH FROM CURRENT_DATE::TIMESTAMP) - 19800

) as five_day

GROUP BY Day, reg, gname

SELECT *
FROM geo_regions
ORDER BY 1



--2. Fetching Data at Vehicle Level

-- 1. finding customer (caller_type 2)calls in the last week, 

SELECT Vname,
	   COUNT(DISTINCT oid) as Total_count,
	   COUNT(DISTINCT oid) FILTER(WHERE ostat = 4) as Completed_Order,
	   COUNT(DISTINCT oid) FILTER(WHERE ostat = 5) as Cancelled_Order

FROM
(
SELECT 
	veh.display_name as Vname,
	ord.order_id as oid,
	ord.status as ostat
	
FROM 
	orders as ord
JOIN
	vehicles as veh
ON 
	ord.vehicle_id = veh.id
WHERE 
	ord.status in (4,5)
	AND
	ord.order_type = 0
	AND
	ord.deleted_at is NULL
	AND	
	ord.pickup_time >= EXTRACT(EPOCH FROM CURRENT_DATE::TIMESTAMP - INTERVAL '5 Days') - 19800
	AND
	ord.pickup_time < EXTRACT(EPOCH FROM CURRENT_DATE::TIMESTAMP) - 19800 
) as t1
GROUP BY 1
