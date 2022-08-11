with raw_data AS (
SELECT 
	id, order_stage_v2, issue_v2, actions_taken_v2, extra_data -> 'Action_Comments__c' as ticket_comment

FROM sf_case_cc_details

WHERE 
		
	created_at BETWEEN
	CURRENT_DATE::TIMESTAMP - INTERVAL '7 Days 5 hours 30 minutes'
	AND
 	CURRENT_DATE::TIMESTAMP - INTERVAL '5 hours 30 minutes'
)


SELECT issue_v2, Escalate_count, Escalate_count / sum(Escalate_count) over()
FROM (
SELECT 
issue_v2, COUNT(DISTINCT id) FILTER(WHERE actions_taken_v2 ILIKE '%Escalate To%') as Escalate_count, count(DISTINCT id) as Total_Count
FROM raw_data 
GROUP BY 1) as raw_data2;



---
with raw_data AS (
SELECT 
	id, order_stage_v2, issue_v2, actions_taken_v2, extra_data -> 'Action_Comments__c' as ticket_comment

FROM sf_case_cc_details

WHERE 
		
	created_at BETWEEN
	CURRENT_DATE::TIMESTAMP - INTERVAL '7 Days 5 hours 30 minutes'
	AND
 	CURRENT_DATE::TIMESTAMP - INTERVAL '5 hours 30 minutes'
)


SELECT issue_v2, 
	Escalate_count, ROUND((Escalate_count / sum(Escalate_count) over())*100,2) as Escal_Percn, 
	Total_Count, ROUND((Escalate_count / sum(Total_Count) over())*100,2) as Total_Percn
FROM (
SELECT 
issue_v2, COUNT(DISTINCT id) FILTER(WHERE actions_taken_v2 ILIKE '%Escalate To%') as Escalate_count, count(DISTINCT id) as Total_Count
FROM raw_data 
GROUP BY 1
) as raw_data2;



--------3.

with raw_data AS (
SELECT 
	id, order_stage_v2, issue_v2, actions_taken_v2, extra_data -> 'Action_Comments__c' as ticket_comment

FROM sf_case_cc_details

WHERE actions_taken_v2 ILIKE '%Escalate To%'
	AND		
	created_at BETWEEN
	CURRENT_DATE::TIMESTAMP - INTERVAL '5 Days 5 hours 30 minutes'
	AND
 	CURRENT_DATE::TIMESTAMP - INTERVAL '5 hours 30 minutes' 
)


	
, raw_data_2 as (SELECT 
issue_v2, count(DISTINCT id) as Escalate_count
FROM raw_data 
GROUP BY 1
ORDER BY 2 DESC
)

SELECT issue_v2, ticket_comment
FROM raw_data
WHERE issue_v2 in (SELECT issue_v2 FROM raw_data_2 where Escalate_count > 500)
ORDER BY 1 ASC;



SELECT ((Escalted_Total)*1.00/Total)
FROM (
SELECT COUNT(DISTINCT id) as Total , COUNT(DISTINCT id) FILTER(WHERE actions_taken_v2 ILIKE '%Escalate To%') as Escalted_Total
FROM sf_case_cc_details
WHERE created_at BETWEEN
	CURRENT_DATE::TIMESTAMP - INTERVAL '5 Days 5 hours 30 minutes'
	AND
 	CURRENT_DATE::TIMESTAMP - INTERVAL '5 hours 30 minutes' 
) as rd ;
