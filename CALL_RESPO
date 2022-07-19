-- 1. finding total number of calls in the last week 

SELECT Day,
		count(DISTINCT customer_crt_id) FILTER (WHERE caller_type in (1,2)) as Total_Calls,
		count(DISTINCT customer_crt_id) FILTER (WHERE caller_type = 1) as Partner_Calls,
		count(DISTINCT customer_crt_id) FILTER (WHERE caller_type = 2) as Customer_Calls
FROM
(
SELECT 
	DATE(response_ts + INTERVAL '5 hours 30 minutes') as Day,
	customer_crt_id,
	customer_id,
	driver_id,
	caller_type
FROM 
	caller_responses
WHERE 
	response_ts >= CURRENT_DATE::TIMESTAMP - INTERVAL '5 Days 5 hours 30 minutes'
	AND
	response_ts < CURRENT_DATE::TIMESTAMP - INTERVAL '5 hours 30 minutes'
) as calls
GROUP BY 1





