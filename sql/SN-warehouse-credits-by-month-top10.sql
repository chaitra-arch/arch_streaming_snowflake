--  Name:         SN-warehouse-credits-by-month-top10.sql  
--  Created Date: 24-April-2023
--  Description:  Took one of the dashboard queries and put it in here to see if it would work in CAS2
------------------------------------------------------------------------------------------
WITH wh_list AS 
-- Configure to how many months back you'd like the top WH USage to be based on
(SELECT warehouse_name, ROUND(SUM(credits_used),0) credits_used FROM snowflake.account_usage.warehouse_metering_history WHERE 
     start_time BETWEEN '{start_date}' AND '{end_date}'
     GROUP BY warehouse_name
     ORDER BY 2 DESC limit 10
                )
, wh_usage_details AS
(
SELECT 
convert_timezone('America/Los_Angeles',current_timestamp()) AS local_cts,
convert_timezone('America/Los_Angeles',start_time) AS local_start_time,
warehouse_name,
DATE_TRUNC('month', local_start_time)::DATE usage_month
,credits_used, credits_used_compute,credits_used_cloud_services
FROM snowflake.account_usage.warehouse_metering_history
WHERE local_start_time BETWEEN '{start_date}' AND '{end_date}'
    AND warehouse_name in
    (SELECT warehouse_name FROM wh_list
     )
)

SELECT usage_month, warehouse_name
,ROUND(SUM(credits_used),0) "WH Credits"
,ROUND(SUM(credits_used_compute),0) AS "Compute Credits"
,ROUND(SUM(credits_used_cloud_services),0) "Cloud Svcs Credits"
,ROUND(SUM(credits_used + credits_used_compute + credits_used_cloud_services), 0) "Total Credits"
FROM wh_usage_details
GROUP BY 1,2
ORDER BY 1;