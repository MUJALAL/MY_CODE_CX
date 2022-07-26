
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
