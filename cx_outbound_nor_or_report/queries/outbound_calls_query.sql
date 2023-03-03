SELECT
    d.id as disposed_call_id,      
    d.created_at,
    d.dialed_time AS dial_time,
    DATE_TRUNC('day',d.dialed_time + INTERVAL '5 hours 30 minutes') AS call_date,
    d.dialed_time + INTERVAL '5 hours 30 minutes' AS dialed_time_ist,
    d.user_id,
    d.dailer_id,                    
    EXTRACT (EPOCH FROM dialed_time)::INTEGER AS dialed_epoch_ts,
    call_type,
    d."customerCRTId" AS customer_crt_id,
    campaign_id,
    system_disposition,
    hangup_desc AS hangup_details,
    RIGHT(d.phone, 10) AS phone
FROM 
    disposed_calls d
WHERE 
    d.created_at >=('{startdate}')::TIMESTAMP -  INTERVAL '5.5 hours'
AND     
    d.id IN {disposed_call_id}
AND 
    campaign_id = '38'

UNION ALL

SELECT
    d.id as disposed_call_id,
    d.created_at,
    d.dialed_time AS dial_time,
    DATE_TRUNC('day',d.dialed_time + INTERVAL '5 hours 30 minutes') AS call_date,
    d.dialed_time + INTERVAL '5 hours 30 minutes' AS dialed_time_ist,
    d.user_id,
    d.dailer_id,                    
    EXTRACT (EPOCH FROM dialed_time)::INTEGER AS dialed_epoch_ts,
    call_type,
    d."customerCRTId" AS customer_crt_id,
    campaign_id,
    system_disposition,
    hangup_desc AS hangup_details,
    RIGHT(d.phone, 10) AS phone   
FROM 
    abandoned_calls d
WHERE 
    d.created_at >=('{startdate}')::TIMESTAMP - INTERVAL '5.5 hours'
AND 
    d.id IN {disposed_call_id}
AND 
    campaign_id = '38'