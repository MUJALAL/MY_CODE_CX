SELECT DISTINCT customer_id 
FROM
(
SELECT  
	customer_id, EXTRACT(MONTH FROM month_start_date), count(order_id)
from completed_spot_orders_fast_mv csofm 
WHERE vehicle_id <> 97 and month_start_date <= '2022-01-31'
GROUP BY 1,2
HAVING count(order_id) >= 1
ORDER BY 1, 2 ASC) as raw_data;
