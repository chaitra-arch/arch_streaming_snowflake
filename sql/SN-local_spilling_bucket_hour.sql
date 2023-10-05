--  Name:         SH-Local_Spilling_Bucket_Hour.sql  
--  Created Date: 24-APR-2023
--  Description:  Grab the number of queries that are bucket spilling over a time frame
------------------------------------------------------------------------------------------
// Local Spilling by Bucket by Hour
use warehouse demo_wh;
--use ROLE ACCOUNTADMIN;
WITH queued_queries AS 
( 
SELECT  
query_id 
,START_TIME 
,round(execution_time / 1000, 2) AS execution_time_sec 
,round(bytes_scanned / power(1024,2), 2) AS megabytes_scanned 
,megabytes_scanned/ NULLIF(execution_time_sec ,0)  AS mb_exec_per_sec 
,round(total_elapsed_time / 1000, 2) AS total_elapsed_time_sec 
, database_name, query_text 
,database_name, schema_name, role_name, warehouse_name, warehouse_size 
,round(queued_overload_time / 1000, 2) AS queued_overload_time_sec 
 ,round(bytes_spilled_to_local_storage / power(1024,3),4) AS gb_spilled_to_remote_storage
     ,CASE WHEN gb_spilled_to_remote_storage > 10.0 THEN TRUE ELSE FALSE END AS SPILLED_MORE_THAN_10GB 
    ,CASE WHEN gb_spilled_to_remote_storage > 5.0 THEN TRUE ELSE FALSE END AS SPILLED_MORE_THAN_5GB 
  ,CASE WHEN gb_spilled_to_remote_storage > 1.0 THEN TRUE ELSE FALSE END AS SPILLED_MORE_THAN_1GB 
  ,CASE WHEN gb_spilled_to_remote_storage > 0.5 THEN TRUE ELSE FALSE END AS SPILLED_MORE_THAN_500MB 
,CASE WHEN gb_spilled_to_remote_storage > 0.1 THEN TRUE ELSE FALSE END AS SPILLED_MORE_THAN_100MB 
    ,CASE WHEN gb_spilled_to_remote_storage > 0.0 THEN TRUE ELSE FALSE END AS SPILLED_MORE_THAN_0MB 
,CASE WHEN SPILLED_MORE_THAN_10GB THEN 'Greater than 10 GB' 
    WHEN SPILLED_MORE_THAN_5GB THEN '5 GB to 10 GB' 
    WHEN SPILLED_MORE_THAN_1GB THEN '1 GB to 5 GB'
    WHEN SPILLED_MORE_THAN_500MB THEN '0.5 GB to 1 GB' -- '500MB to 1GB' 
    WHEN SPILLED_MORE_THAN_100MB THEN '0.1 GB to 0.5 GB' --'100MB to 500MB' 
    WHEN SPILLED_MORE_THAN_0MB THEN '< 0.1 GB' -- '< 100MB'
    ELSE 'None' END AS REMOTE_SPILLING_BUCKET  
FROM SNOWFLAKE.account_usage.query_history qh 
WHERE start_time > DATEADD('week',-1, CURRENT_DATE) 
AND WAREHOUSE_NAME = :warehouse_name 
--AND total_elapsed_time_sec > 5.0  -- 5 seconds 
AND execution_time_sec >= 1.0
), grouped_results AS 
(SELECT WAREHOUSE_NAME,  
 DATE_TRUNC('day', START_TIME)::datetime AS query_date,  
to_char(convert_timezone('America/Los_Angeles', 'America/Chicago', start_time::timestamp_ntz), 'MM/DD/YYYY HH24') as cst_date, 
 REMOTE_SPILLING_BUCKET, COUNT(*) AS queries_run, AVG(gb_spilled_to_remote_storage) AS gb_spilled_to_remote_storage
FROM queued_queries 
GROUP BY 1,2,3,4) 
SELECT warehouse_name, cst_date, REMOTE_SPILLING_BUCKET, queries_run, pct_of_total, gb_spilled_to_remote_storage AS avg_gb_spilled_to_remote_storage FROM
(SELECT warehouse_name, cst_date, REMOTE_SPILLING_BUCKET, sum(queries_run) AS queries_run , AVG(gb_spilled_to_remote_storage) AS gb_spilled_to_remote_storage
,ROUND(100 * ratio_to_report(sum(queries_run)) over (PARTITION BY warehouse_name, cst_date) ,2) AS pct_of_total 
    FROM grouped_results 
    GROUP BY 1,2,3)
WHERE REMOTE_SPILLING_BUCKET <> 'None'
ORDER BY 1,2,3;

