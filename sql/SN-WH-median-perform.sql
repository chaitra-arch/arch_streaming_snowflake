--  Name:         SN-WH-median-perform.sql 
--  Created Date: 11-May-2023
--  Description:  https://snowflakecomputing.atlassian.net/wiki/spaces/SKE/pages/2323415216/Health+check+of+Compute+-+Customer+Account
-- Warehouse Concurrency
------------------------------------------------------------------------------------------

/* Warehouse Median Performance*/
SELECT warehouse_name,
warehouse_size,
//:datebucket(end_time) query_end_date,
COUNT(*) queries, 
ROUND(AVG(total_elapsed_time/1000),0) as avg_query_time
,ROUND(MEDIAN(total_elapsed_time/1000),0) as Median_Query_Time
--,ROUND(avg_query_time/Median_Query_Time , 1) as median_ratio
, ROUND(AVG(execution_time/1000),2) as avg_execution_time
, ROUND(MEDIAN(execution_time/1000),2) as MedianExecutionTime
, ROUND(MEDIAN(compilation_time/1000),2) as MedianCompilationTime
, ROUND(MEDIAN(queued_overload_time/1000),2) as MedianQueuedTime
FROM SNOWFLAKE.account_usage.query_history
WHERE start_time  >= dateadd('days', -30, current_date())--:daterange
and (WAREHOUSE_NAME not like 'COMPUTE_SERVICE%')
and (warehouse_size is not null)
GROUP BY 1,2 //,2
ORDER BY 4 desc,1,2 //, 3
limit 30
;