-- 1. At Issue Level 

SELECT 
	 Reason,
	 count(DISTINCT csid) as Total_Tickets,
	 count(DISTINCT csid) FILTER(WHERE status = 'Open') as Total_Open_Tickets,
	 count(DISTINCT csid) FILTER(WHERE status = 'Closed') as Total_Closed_Tickets,
	 count(DISTINCT csid) FILTER(WHERE raised_by = 'Partner') as Total_Partner_Tickets,
	 count(DISTINCT csid) FILTER(WHERE raised_by = 'Customer') as Total_Customer_Tickets
	 
FROM
(
SELECT
	sfc.id as csid,
	raised_by,
	sfc.status,
	case_type,
	unnest(string_to_array(issue_v2,';')) as Reason
FROM 
	sf_cases as sfc
JOIN
	sf_case_cc_details as sfcd
ON 
	sfc.id = sfcd.case_cc_id
) as t1
GROUP BY 1



-- 2. At Partner and Customer Level 

SELECT 
	 raised_by,
	 Reason,
	 count(DISTINCT csid) FILTER(WHERE status in ('Open', 'Closed')) as Total_Tickets,
	 count(DISTINCT csid) FILTER(WHERE status = 'Open') as Total_Open_Tickets,
	 count(DISTINCT csid) FILTER(WHERE status = 'Closed') as Total_Closed_Tickets
FROM
(
SELECT
	sfc.id as csid,
	raised_by,
	sfc.status,
	case_type,
	unnest(string_to_array(issue_v2,';')) as Reason
FROM 
	sf_cases as sfc
JOIN
	sf_case_cc_details as sfcd
ON 
	sfc.id = sfcd.case_cc_id
) as t1
WHERE raised_by in ('Customer', 'Partner')
GROUP BY 1,2

-- 3. At Geo Region Level

--  finding partner tickets and which issue they belonged too, same with customers

SELECT 
	 genam,
	 count(DISTINCT csid) FILTER(WHERE status in ('Open', 'Closed')) as Total_open_close_Tickets,
	 count(DISTINCT csid) FILTER(WHERE status = 'Open') as Total_Open_Tickets,
	 count(DISTINCT csid) FILTER(WHERE status = 'Closed') as Total_Closed_Tickets,
	 count(DISTINCT csid) FILTER(WHERE raised_by in ('Customer', 'Partner')) as Total_Tickets,
	 count(DISTINCT csid) FILTER(WHERE raised_by  = 'Customer') as Total_Customer,
	 count(DISTINCT csid) FILTER(WHERE raised_by  = 'Partner') as Total_Partner
	 
FROM
(
SELECT
	sfc.id as csid,
	geo_region_name as genam,
	raised_by,
	sfc.status,
	case_type,
	unnest(string_to_array(issue_v2,';')) as Reason
FROM 
	sf_cases as sfc
JOIN
	sf_case_cc_details as sfcd
ON 
	sfc.id = sfcd.case_cc_id
) as t1
WHERE raised_by in ('Customer', 'Partner')
GROUP BY 1
 
