SELECT
--     incoming_customer_crt_id AS customer_crt_id,
    outbound_request_log_id AS id,
    disposition_id AS disposed_call_id,
    disposition_type
FROM
    outbound_call_dispositions
WHERE
    outbound_request_log_id IN {request_id}
    