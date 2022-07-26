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
