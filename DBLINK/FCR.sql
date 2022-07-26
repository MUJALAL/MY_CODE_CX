with rds_data as (
    SELECT
        *
    FROM
        dblink('host=sfms-prod-psql-replica.porter.in port=5432 user=yashasvi_pankaj password=5hOMr9pWfZxQ3YKtRSbVIZE+JW8= dbname=sfms_production',
            '
          SELECT 
          date_trunc(''month'',sf_cases.created_at + INTERVAL ''5.5 hours'') as month,
          geo_region_name,
          order_stage_v2, 
          issue_v2,
          raised_by,
          crn,
case when (extract(epoch from sf_cases.closed_at)::integer - extract(epoch from sf_cases.created_at)::integer <= 14400) then 1 else 0 end as resolved_in_Four_hours,
case when (extract(epoch from sf_cases.closed_at)::integer - extract(epoch from sf_cases.created_at)::integer <= 86400) then 1 else 0 end as resolved_in_a_day,
case when (extract(epoch from sf_cases.closed_at)::integer - extract(epoch from sf_cases.created_at)::integer <= 172800) then 1 else 0 end as resolved_in_2_day,
case when (extract(epoch from sf_cases.closed_at)::integer - extract(epoch from sf_cases.created_at)::integer <= 10) then 1 else 0 end as fcr,
count(*) as tickets

                   FROM
                   sf_cases
                   LEFT JOIN sf_case_cc_details sf_cc ON sf_cc.case_cc_id = sf_cases.id
                   WHERE
                   sf_cases.created_at   >= ''2022-07-01'' :: TIMESTAMP - INTERVAL ''5.5 hours''
                   AND sf_cases.created_at <= ''2022-07-31'' :: TIMESTAMP + INTERVAL ''1 day'' - INTERVAL ''5.5 hours''
                   GROUP BY
                   1,2,3,4,5,6,7,8,9,10
                   ') as t1 (month timestamp,
                         geo_region_name varchar,
                         order_stage_v2 varchar,
                         issue_v2 varchar,
                         raised_by varchar,
                         crn varchar,
                         resolved_in_Four_hours int,
                         resolved_in_a_day int,
                         resolved_in_2_day int,
                         fcr int,
                         tickets bigint )
    ),
    
    
    order_level_info AS (
    SELECT
        rd.*,
        o.geo_region_id,
        o.vehicle_id,
        o.order_id as crn_id,
        o.created_at as order_created
    FROM rds_data rd
         JOIN orders o ON rd.crn = o.order_id
        )

        select * from order_level_info;


