SELECT 
        caller_responses.order_id,
        RANK() OVER (PARTITION BY caller_responses.order_id ORDER BY call_id) AS call_sequence,
        COUNT(call_id) OVER (PARTITION BY caller_responses.order_id) AS total_attempts,
        call_id,
        call_list_id,
        dtmf_inputs,
        caller_responses.ivr,
        LEFT(dtmf_inputs,1) AS first_step,
        SUBSTRING(dtmf_inputs,3,1) AS SECOND_step,
        RIGHT(dtmf_inputs,1) AS last_step,
        (LENGTH(dtmf_inputs) - LENGTH(REPLACE(dtmf_inputs, ',', '')))+1 AS total_steps,
        o.geo_region_id,
        geo_regions."name"

    FROM 
        abandoned_calls ac
    LEFT JOIN 
        caller_responses ON ac."customerCRTId" = caller_responses.customer_crt_id
    LEFT JOIN
        orders o ON caller_responses.order_id = o.id
    LEFT JOIN
        geo_regions ON o.geo_region_id = geo_regions.id
    WHERE 
        caller_responses.ivr ILIKE '%transit%'
    AND
        caller_responses.ivr NOT ILIKE '%customer%'
    AND
        caller_responses.caller_type = 1
    AND 
            o.geo_region_id IS NOT NULL
           LIMIT 100;
--     AND dialed_time + INTERVAL '5.5 hours' BETWEEN;
;
WITH 
-- time_period AS
-- (
--     SELECT 
--         {{start_date}}::TIMESTAMP AS start_date,
--         {{end_date}}::TIMESTAMP AS end_date
-- ),
raw_data AS
(
SELECT 
        cr.order_id,
        RANK() OVER (PARTITION BY cr.order_id ORDER BY call_id) AS call_sequence,
        COUNT(call_id) OVER (PARTITION BY cr.order_id) AS total_attempts,
        call_id,
        call_list_id,
        dtmf_inputs,
        cr.ivr,
        LEFT(dtmf_inputs,1) AS first_step,
        SUBSTRING(dtmf_inputs,3,1) AS SECOND_step,
        RIGHT(dtmf_inputs,1) AS last_step,
        (LENGTH(dtmf_inputs) - LENGTH(REPLACE(dtmf_inputs, ',', '')))+1 AS total_steps,
        o.geo_region_id,
        gr."name"

FROM 
	abandoned_calls ac

LEFT JOIN 
	caller_responses cr 
		ON ac."customerCRTId" = cr.customer_crt_id

LEFT JOIN
	orders o 
		ON cr.order_id = o.id

LEFT JOIN
	geo_regions gr 
        ON o.geo_region_id = gr.id

WHERE
	cr.ivr ILIKE '%drop%'
AND
	cr.ivr NOT ILIKE '%customer%'
AND
	cr.caller_type = 1
AND 
	o.geo_region_id IS NOT NULL
),

final_data AS
(
    SELECT 
        *, 
        CASE 
            WHEN (ivr ILIKE '%drop%' AND first_step  = '1') AND (last_step = '1' OR last_step = '2' OR last_step = '3' OR last_step = '4' OR last_step = '5' OR last_step = '6') AND 
                            total_steps> 1 THEN 1
            WHEN (ivr ILIKE '%drop%' AND first_step  = '2') AND (last_step = '9' OR last_step = '1') AND total_steps> 1 THEN 1
        ELSE 0 END AS complete_string,
        
        -- auto resolved
        CASE
            WHEN (ivr ILIKE '%drop%' AND first_step = '1' AND (last_step= '1' OR last_step = '2' OR last_step = '3' OR last_step = '4')) THEN 1
            WHEN (ivr ILIKE '%drop%' AND ((first_step = '2' AND last_step= '1') OR (first_step = '2' AND last_step = '9'))) THEN 1
        ELSE 0 END AS auto_resolved
        
    FROM raw_data
)

SELECT *
FROM final_data;
SELECT 
    CASE
        WHEN ivr ILIKE '%drop%'
            THEN 'IVR Drop'
    END AS order_status,
    COUNT(call_id) as total_calls,
    (COUNT(CALL_id) FILTER (WHERE total_attempts =1))*100.0/COUNT(call_id) AS pct_one_time_callers,
    (COUNT(CALL_id) FILTER (WHERE total_attempts =1 AND dtmf_inputs = ''))*100.0/COUNT(call_id) AS pct_one_time_callers_no_input,
    (COUNT(CALL_id) FILTER (WHERE total_attempts =1 AND dtmf_inputs != '' AND complete_String = 1))*100.0/COUNT(call_id) AS pct_one_time_callers_flow_complete,
    (COUNT(CALL_id) FILTER (WHERE total_attempts =1 AND dtmf_inputs != '' AND complete_String = 0))*100.0/COUNT(call_id) AS pct_one_time_callers_dropoff,
    (COUNT(CALL_id) FILTER (WHERE total_attempts >1))*100.0/COUNT(call_id) AS pct_repeated_callers,
    (COUNT(CALL_id) FILTER (WHERE total_attempts >1 AND dtmf_inputs = ''))*100.0/COUNT(call_id) AS pct_repeated_callers_no_input,
    (COUNT(CALL_id) FILTER (WHERE total_attempts >1 AND dtmf_inputs != '' AND complete_String = 1))*100.0/COUNT(call_id) AS pct_repeated_callers_flow_complete,
    (COUNT(CALL_id) FILTER (WHERE total_attempts >1 AND dtmf_inputs != '' AND complete_String = 0))*100.0/COUNT(call_id) AS pct_repeated_callers_dropoff,
    COUNT(DISTINCT CALL_list_id) FILTER (WHERE total_attempts >1) AS total_repeat_users 
FROM final_data
GROUP BY 1;
