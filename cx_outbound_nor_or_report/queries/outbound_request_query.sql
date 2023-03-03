SELECT 
    *,
    DATE_TRUNC('second', created_at + INTERVAL '5.5 hours') AS requested_time_ist,
    EXTRACT(EPOCH FROM created_at):: INTEGER AS epoch_requested_time
FROM
    outbound_request_logs
WHERE 
    created_at + INTERVAL '5.5 hours' >= ('{startdate}')::TIMESTAMP
AND
    created_at + INTERVAL '5.5 hours' < CURRENT_DATE::TIMESTAMP