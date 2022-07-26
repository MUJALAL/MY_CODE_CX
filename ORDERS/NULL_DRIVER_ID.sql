
SELECT 
                 order_week,
                 a.status,
                 a.id,
            	 crr.name,
            	 crr.id

FROM (

SELECT
                 orders.id,
                 cancel_reason_id,
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
                 extract(epoch FROM date_trunc('week',current_date))::INTEGER - 19800 - 7*86400
                 AND extract(epoch FROM date_trunc('week',current_date))::INTEGER - 19801
                 AND driver_id is null
                 ORDER BY cancel_reason_id 
                ) AS a
LEFT JOIN caller_responses cr 
ON a.id = cr.order_id
LEFT JOIN cancel_reasons crr 
ON crr.id = a.cancel_reason_id
and crr.id not in (68,70,80,79);
