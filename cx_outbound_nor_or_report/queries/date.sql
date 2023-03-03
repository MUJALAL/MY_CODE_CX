SELECT 
    MAX(DATE_TRUNC('day', date)) + INTERVAL '1 day' AS date
FROM 
    anant.murt_ob_summary