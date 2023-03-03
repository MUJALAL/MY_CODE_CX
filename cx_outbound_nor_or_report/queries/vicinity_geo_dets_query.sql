SELECT 
    DISTINCT customer_crt_id as source_id,
    cr.order_id,
    cr.driver_id,
    cr.queue_name,
    cr.caller_type,
    cr.ivr,
    caller_info:: json ->> 'user_city' AS city,
    caller_info:: json ->> 'channel' AS channel,
    ord.vehicle_id, 
    ord.status,
    ac.dtmf_inputs,
    dr.vehicle_id as driver_vehicle,
    ord.order_id as crn
    
FROM
    caller_responses cr 
LEFT JOIN 
    orders ord 
ON cr.order_id = ord.id
LEFT JOIN 
    abandoned_calls ac 
ON cr.customer_crt_id = ac."customerCRTId"
LEFT JOIN 
    drivers dr 
ON dr.id = cr.driver_id
WHERE
    customer_crt_id IN {sourceid}