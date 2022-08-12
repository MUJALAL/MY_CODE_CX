with raw_data as(
SELECT sfc.id, order_stage_v2, issue_v2, actions_taken_v2, extra_data -> 'Action_Comments__c' as ticket_comment, geo_region_name, sfcd.created_at
FROM sf_case_cc_details sfcd
JOIN sf_cases sfc
ON sfcd.case_cc_id = sfc.id 
WHERE 
 actions_taken_v2 ILIKE '%Escalate To%'
	AND
	sfc.created_at BETWEEN
		('2022-08-06'::TIMESTAMP - INTERVAL ' 5 hours 30 minutes')
			AND
 				('2022-08-10'::TIMESTAMP - INTERVAL '5 hours 30 minutes')
)


,raw_data_2 as (SELECT 
 issue_v2, count(DISTINCT id) as Escalate_count
FROM raw_data 
GROUP BY 1
HAVING count(DISTINCT id) > 1000)

SELECT issue_v2, ticket_comment
FROM raw_data
WHERE issue_v2 in (SELECT issue_v2 FROM raw_data_2)
ORDER BY issue_v2;
