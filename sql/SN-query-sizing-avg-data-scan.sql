--  Name:         SN-WH-query-sizing-avg-data-scan.sql  
--  Created Date: 24-April-2023
--  Description:  Took one of the dashboard queries and put it in here to see if it would work in CAS2
------------------------------------------------------------------------------------------
SELECT warehouse_name, warehouse_size
, date_trunc('day', convert_timezone('America/Los_Angeles', 'America/Chicago', end_time::timestamp_ntz)) end_date_cst
 ,TO_CHAR(ROUND(avg(BYTES_SCANNED) / POWER(2, 20), 3)) as avg_query_size-- Average number of MB per query
  ,TO_CHAR(ROUND(median(BYTES_SCANNED) / POWER(2, 20), 3)) as med_query_size-- Average number of MB per query
,       CASE
         WHEN avg(BYTES_SCANNED) >= POWER(2, 40) THEN TO_CHAR(ROUND(avg(BYTES_SCANNED) / POWER(2, 40), 1)) || ' Tb'
         WHEN avg(BYTES_SCANNED) >= POWER(2, 30) THEN TO_CHAR(ROUND(avg(BYTES_SCANNED) / POWER(2, 30), 1)) || ' Gb'
         WHEN avg(BYTES_SCANNED) >= POWER(2, 20) THEN TO_CHAR(ROUND(avg(BYTES_SCANNED) / POWER(2, 20), 1)) || ' Mb'
         WHEN avg(BYTES_SCANNED) >= POWER(2, 10) THEN TO_CHAR(ROUND(avg(BYTES_SCANNED) / POWER(2, 10), 1)) || ' K'
         ELSE TO_CHAR(round(avg(BYTES_SCANNED)))                        -- Average number of bytes scanned
       END AS AVG_BYTES_SCANNED  
,       round(avg(execution_time)/1000,0) as avg_exe_secs               -- Average execution time in seconds
,       round(avg(queued_overload_time)/1000,0) as avg_wait_secs        -- Average wait time in seconds 
,       count(*) as count_total                                         -- Total Queries
,       count(decode(q.QUEUED_OVERLOAD_TIME,0,null,1)) as count_wait    -- Total Queries waited
,       round(count_wait / count_total * 100) as pct_wait_count         -- Percentage of waiting queries
, round(avg(bytes_scanned)) as avg_bytes
, round(median(bytes_scanned)) as med_bytes
FROM snowflake.account_usage.query_history q 
WHERE warehouse_size is not NULL
AND bytes_scanned > 0 --- eliminates Result Cache Hits
AND   end_time BETWEEN  '{start_date}' AND '{end_date}'
GROUP BY warehouse_name, end_date_cst, warehouse_size
ORDER BY warehouse_name, end_date_cst ASC;