SELECT
order_week::DATE AS order_week,
                 status,
                 COUNT(id) AS total_orders,
                 COUNT(id) FILTER (WHERE COUNT>0) AS orders_with_calls,
                 COUNT(id) FILTER (WHERE COUNT_of_non_ivr_calls>0) AS orders_with_non_ivr_calls,
                 avg(count) filter(WHERE count>0) AS AVG_enquires,
                 avg(count) filter(WHERE COUNT_of_inbound_calls>0) AS avg_inbound_enquires,
                 avg(count) filter(WHERE COUNT_of_outbound_calls>0) AS avg_outbound_enquires,
                 avg(count) filter(WHERE COUNT_of_non_ivr_calls>0) AS AVG_non_ivr_enquires,
                 avg(count) filter(WHERE COUNT_of_non_ivr_inbound_calls>0) AS AVG_non_ivr_inbound_enquires,
                 avg(count) filter(WHERE COUNT_of_non_ivr_outbound_calls>0) AS AVG_non_ivr_outbound_enquires,
                 COUNT(id) FILTER (
          		 WHERE
                 COUNT > 0
                 ) * 1.00 / COUNT(id) AS perc_orders_with_calls,
                 COUNT(id) FILTER (
                 WHERE
                 COUNT_of_inbound_calls > 0
                 ) * 1.00 / COUNT(id) AS perc_orders_with_inbound_calls,
                 COUNT(id) FILTER (
                 WHERE
                 COUNT_of_outbound_calls > 0
                 ) * 1.00 / COUNT(id) AS perc_orders_with_outbound_calls,
                 
                 COUNT(id) FILTER (
                 WHERE
                 COUNT_of_non_ivr_calls > 0
                 ) * 1.00 / COUNT(id) AS perc_orders_with_non_ivr_calls,
                 
                 COUNT(id) FILTER (
                 WHERE
                 COUNT_of_non_ivr_inbound_calls > 0
                 ) * 1.00 / COUNT(id) AS perc_orders_with_non_ivr_inbound_calls,
                 
                 COUNT(id) FILTER (
                 WHERE
                 COUNT_of_non_ivr_outbound_calls > 0
                 ) * 1.00 / COUNT(id) AS perc_orders_with_non_ivr_outbound_calls
                 
                 
                 FROM
                 (
                 SELECT
                 order_week,
                 status,
                 a.id,
                 COUNT(DISTINCT customer_crt_id),
                 COUNT(DISTINCT customer_crt_id) FILTER (WHERE ivr = 'none') AS COUNT_of_non_ivr_calls,
                 COUNT(DISTINCT customer_crt_id) FILTER (WHERE cr.caller_info -> 'channel' ='inbound') AS COUNT_of_inbound_calls,
                 COUNT(DISTINCT customer_crt_id) FILTER (WHERE cr.caller_info -> 'channel' = 'outbound') AS COUNT_of_outbound_calls,
                 COUNT(DISTINCT customer_crt_id) FILTER (WHERE cr.caller_info -> 'channel' = 'inbound' AND ivr = 'none') AS COUNT_of_non_ivr_inbound_calls,
                 COUNT(DISTINCT customer_crt_id) FILTER (WHERE cr.caller_info -> 'channel' = 'outbound' AND ivr = 'none') AS COUNT_of_non_ivr_outbound_calls
                 FROM
                 (
                 SELECT
                 orders.id,
                 case when orders.status = 5 and cancel_reason_id not in (68,70,80,79) then 'cancelled without attribution porter' when orders.status = 5 then 'cancelled with attribution porter' else 'completed' end as status ,
                 to_timestamp(pickup_time) + INTERVAL '5.5 hours' AS order_time_ist,
                 DATE_trunc(
                 'week',
                 to_timestamp(pickup_time) + INTERVAL '5.5 hours'
                 ) AS order_week
                 FROM
                 orders                          
                 WHERE
                 order_type = 0
                 AND orders.status in (4,5)
                 AND pickup_time BETWEEN
                 extract(epoch FROM date_trunc('week',current_date))::INTEGER - 19800 - 70*86400
                 AND extract(epoch FROM date_trunc('week','2022-07-14'::DATE))::INTEGER - 19801 
                 ) a
                 LEFT JOIN caller_responses cr ON a.id = cr.order_id
                 GROUP BY
                 1,
                 2,
                 3
                 ) aa
                 GROUP BY
                 1,
                 2
                 ORDER BY
                 1,2;
