--  Name:         SN-query-queue-by-bucket.sql  
--  Created Date: 24-April-2023
--  Description:  Took one of the dashboard queries and put it in here to see if it would work in CAS2
------------------------------------------------------------------------------------------
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
,CASE WHEN queued_overload_time_sec > 30 THEN TRUE ELSE FALSE END AS QUEUED_MORE_THAN_3OS 
,CASE WHEN queued_overload_time_sec > 10 THEN TRUE ELSE FALSE END AS QUEUED_MORE_THAN_1OS 
,CASE WHEN queued_overload_time_sec > 5 THEN TRUE ELSE FALSE END AS QUEUED_MORE_THAN_5S 
,CASE WHEN QUEUED_MORE_THAN_3OS THEN '>30s' WHEN QUEUED_MORE_THAN_1OS THEN '10s to 30s' WHEN QUEUED_MORE_THAN_5S THEN ' 5s to 10s' ELSE 'None' END AS QUEUE_BUCKET  
FROM SNOWFLAKE.account_usage.query_history qh 
WHERE start_time BETWEEN  '{start_date}' AND '{end_date}'
--AND total_elapsed_time_sec > 5.0  -- 5 seconds 
AND execution_time_sec >= 1.0 
)  
, grouped_results AS 
(SELECT WAREHOUSE_NAME,  
 DATE_TRUNC('day', START_TIME)::datetime AS query_date,  
 date_trunc('week', convert_timezone('America/Chicago', query_date)) cst_date, 
 QUEUE_BUCKET, COUNT(*) AS queries_run 
FROM queued_queries 
GROUP BY 1,2,3,4) 
SELECT warehouse_name, cst_date, QUEUE_BUCKET,queries_run, pct_of_total FROM
(SELECT warehouse_name, cst_date, QUEUE_BUCKET, sum(queries_run) AS queries_run 
,ROUND(100 * ratio_to_report(sum(queries_run)) over (PARTITION BY warehouse_name, cst_date) ,2) AS pct_of_total 
    FROM grouped_results 
    GROUP BY 1,2,3)
WHERE queue_bucket <> 'None'
ORDER BY 1,2,3;