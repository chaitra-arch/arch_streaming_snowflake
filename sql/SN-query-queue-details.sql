--  Name:         SN-query-queue-details.sql  
--  Created Date: 24-April-2023
--  Description:  Took one of the dashboard queries and put it in here to see if it would work in CAS2
------------------------------------------------------------------------------------------
SELECT D.RUN_DATE 
       ,D.warehouse_name
       ,D.query_text 
       ,avg(D.megabytes_scanned) as MB_SCANNED_AVG
       ,avg(D.total_elapsed_time_sec) as ELAPSED_TIME_SEC_AVG
       ,avg(D.queued_overload_time_sec) as QUEUED_TIME_SEC_AVG
FROM (SELECT TO_DATE(start_time) as RUN_DATE
,query_text 
,warehouse_name
,warehouse_size 
,round(execution_time / 1000, 2) AS execution_time_sec 
,round(bytes_scanned / power(1024,2), 2) AS megabytes_scanned 
,megabytes_scanned/ NULLIF(execution_time_sec ,0)  AS mb_exec_per_sec 
,round(total_elapsed_time / 1000, 2) AS total_elapsed_time_sec
,round(queued_overload_time / 1000, 2) AS queued_overload_time_sec 
 FROM SNOWFLAKE.account_usage.query_history qh 
WHERE start_time BETWEEN  '{start_date}' AND '{end_date}'
  AND QUERY_TEXT IS NOT NULL
  AND TRIM(QUERY_TEXT) IS NOT NULL
  AND execution_time_sec >= 1.0 ) as D
GROUP BY D.RUN_DATE, D.warehouse_name, D.query_text
ORDER BY 5, 1, 2
