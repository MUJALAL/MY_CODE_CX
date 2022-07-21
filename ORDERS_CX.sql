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


--2. Per Region

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



--3. LAST 4 WEEKS OF DATA Total Number of Calls, Orders and Call Per Orders at Reason and Weekely level.
with raw_data as (

SELECT 
		t1.*, cr.customer_crt_id crcrtid, ivr, caller_type, driver_id, customer_id, order_id, queue_name 
FROM
(
SELECT
	ord.id, ord.cancel_reason_id, ord.status,
	
	CASE WHEN cancel_reason_id not in (68,70,80,79) THEN 'cancelled without attribution porter' ELSE 'cancelled with attribution porter' END AS status_rep,
	
	to_timestamp(pickup_time) + INTERVAL '5.5 hours' AS order_time_ist,
    
    DATE_trunc('week',to_timestamp(pickup_time) + INTERVAL '5.5 hours') AS order_week,
    
    geo_region_id, vehicle_id
FROM 
	orders as ord
WHERE 
	ord.status = 5
	AND
	ord.order_type = 0
	AND
	pickup_time 
		BETWEEN 
			EXTRACT(EPOCH FROM date_trunc('week',CURRENT_DATE))::INTEGER - 19800 - 28*86400
		AND
			EXTRACT(EPOCH FROM date_trunc('week',CURRENT_DATE))::INTEGER - 19801
) AS t1
LEFT JOIN caller_responses cr 
	 ON cr.order_id = t1.id	 

)


, filt_call_types as (
SELECT crtid, campaign_id, call_id, dtmf_inputs, 'abandoned' as call_type, created_at
FROM(
SELECT crtid, campaign_id, call_id, dtmf_inputs, 'abandoned' as call_type, created_at, rank() over(partition by crtid order by created_at DESC) as RNK
FROM(
SELECT  "customerCRTId" as crtid, campaign_id, id as call_id, dtmf_inputs, 'abandoned' as call_type, created_at
FROM abandoned_calls
WHERE "customerCRTId" in (SELECT crcrtid
			FROM raw_data)


UNION ALL

SELECT "customerCRTId" as crtid,campaign_id, id as call_id,'' as  dtmf_inputs, 'disposed' as call_type, created_at
from disposed_calls
WHERE "customerCRTId" in (SELECT crcrtid
			FROM raw_data)

) as t3
) as filt_call
WHERE RNK = 1
)

SELECT *, ROUND((Total_Calls * 100.0) / NULLIF((Total_Orders * 100.0), 0),2) as Call_Per_Orders 
FROM (
SELECT name,order_week, 
			count(DISTINCT order_id) AS Total_Orders,
			count(DISTINCT call_id) AS Total_Calls
FROM
(
SELECT rw.*,
		 fct.*, 
			vscr.attribution, vscr.name, vscr.source, vscr.id as cancel_reason_id
FROM raw_data rw
LEFT JOIN filt_call_types fct
ON rw.crcrtid = fct.crtid
LEFT JOIN view_spot_product_cancel_reasons vscr
ON vscr.id = rw.cancel_reason_id
) 
as final
GROUP BY 1,2
) 
AS ord_cancelled
ORDER BY order_week;
