with t1 as (SELECT *
FROM caller_responses
WHERE created_at BETWEEN '2022-10-01'::TIMESTAMP - INTERVAL '5.5 hours' and '2022-11-01'::TIMESTAMP - INTERVAL '5.5 hours')

,t2 as(SELECT t1.*, ord.id as oid , ord.vehicle_id as ovid, dr.id as ddid, dr.vehicle_id as dvid
FROM t1
LEFT JOIN orders ord 
ON t1.order_id = ord.id
LEFT JOIN drivers dr
on dr.id = t1.driver_id
)

-- SELECT t1.customer_crt_id
-- FROM t1
-- LEFT JOIN abandoned_calls ac
-- ON ac."customerCRTId" = t1.customer_crt_id
-- LEFT JOIN disposed_calls dc
-- ON dc."customerCRTId" = t1.customer_crt_id
-- WHERE ac."customerCRTId" is null and dc."customerCRTId" is null
-- ;

,t3 as (
SELECT  
		"customerCRTId",  system_disposition, 
		campaign_id, call_type, '0' as talk_time, '0' as hold_time, created_at + INTERVAL '5.5 hours' AS date
FROM 
	abandoned_calls
WHERE created_at BETWEEN '2022-10-01'::TIMESTAMP - INTERVAL '5.5 hours' and '2022-11-01'::TIMESTAMP - INTERVAL '5.5 hours'


UNION ALL

SELECT 
		"customerCRTId", system_disposition,
		campaign_id, call_type, talk_time, hold_time, created_at + INTERVAL '5.5 hours' AS date	
FROM 
	disposed_calls	
WHERE created_at BETWEEN '2022-10-01'::TIMESTAMP - INTERVAL '5.5 hours' and '2022-11-01'::TIMESTAMP - INTERVAL '5.5 hours')


, cte as (SELECT *
FROM t2
INNER JOIN t3
ON t2.customer_crt_id = t3."customerCRTId")



SELECT 	count(DISTINCT customer_crt_id) as total_inbound_Calls,
		round((sum(talk_time + hold_time) / count(DISTINCT customer_crt_id)) / 1000.00,2) AS total_inbound_Calls_aht,
		
		count(DISTINCT customer_crt_id) FILTER (WHERE order_id is not null) as or_calls,
		round((sum(talk_time + hold_time) FILTER (WHERE order_id is not null) / count(DISTINCT customer_crt_id) FILTER (WHERE order_id is not null)) / 1000.00,2) AS or_calls_aht,
		
		count(DISTINCT customer_crt_id) FILTER (WHERE order_id is not null and ovid = 97) as or_two_wheeler_calls,
		round((sum(talk_time + hold_time) FILTER (WHERE order_id is not null and ovid = 97) / count(DISTINCT customer_crt_id) FILTER (WHERE order_id is not null and ovid = 97)) / 1000.00,2)  AS or_two_wheeler_calls_aht,
		
		
		count(DISTINCT customer_crt_id) FILTER (WHERE order_id is not null and  ovid != 97) as or_trucks_calls,
		round((sum(talk_time + hold_time) FILTER (WHERE order_id is not null and  ovid != 97) / count(DISTINCT customer_crt_id) FILTER (WHERE order_id is not null and  ovid != 97)) / 1000.00,2) AS or_trucks_calls_aht,
		
		
		count(DISTINCT customer_crt_id) FILTER (WHERE order_id is null) as nor_calls,
		round((sum(talk_time + hold_time) FILTER (WHERE order_id is null) / count(DISTINCT customer_crt_id) FILTER (WHERE order_id is null)) / 1000.00,2) AS nor_calls_aht,
		
		count(DISTINCT customer_crt_id) FILTER (WHERE order_id is null and driver_id is not null) as nor_partner_calls,
		round((sum(talk_time + hold_time) FILTER (WHERE order_id is null and driver_id is not null) / count(DISTINCT customer_crt_id) FILTER (WHERE order_id is null and driver_id is not null)) / 1000.00,2) AS nor_partner_calls_aht,
		
		count(DISTINCT customer_crt_id) FILTER (WHERE order_id is null and driver_id is not null and  dvid = 97) as nor_partner_two_wheeler_calls,
		round((sum(talk_time + hold_time) FILTER (WHERE order_id is null and driver_id is not null and  dvid = 97) / count(DISTINCT customer_crt_id) FILTER (WHERE order_id is null and driver_id is not null and  dvid = 97)) / 1000.00,2) AS nor_partner_two_wheeler_calls_aht,
		
		count(DISTINCT customer_crt_id) FILTER (WHERE order_id is null and driver_id is not null and  dvid != 97) as nor_partner_trucks_calls,
		round((sum(talk_time + hold_time) FILTER (WHERE order_id is null and driver_id is not null and  dvid != 97)) / (count(DISTINCT customer_crt_id) FILTER (WHERE order_id is null and driver_id is not null and  dvid != 97)) / 1000.00,2) AS nor_partner_trucks_calls_aht,
		
		count(DISTINCT customer_crt_id) FILTER (WHERE order_id is null and customer_id is not null) as nor_customer_calls,
		round((sum(talk_time + hold_time) FILTER (WHERE order_id is null and customer_id is not null) / count(DISTINCT customer_crt_id) FILTER (WHERE order_id is null and customer_id is not null)) / 1000.00,2) AS nor_customer_calls_aht,
		
		count(DISTINCT customer_crt_id) FILTER (WHERE order_id is null and driver_id is null and customer_id is null) as unknown_calls,
		round((sum(talk_time + hold_time) FILTER (WHERE order_id is null and driver_id is null and customer_id is null) / count(DISTINCT customer_crt_id) FILTER (WHERE order_id is null and driver_id is null and customer_id is null)) / 1000.00,2) AS unknown_calls_aht
FROM cte;
