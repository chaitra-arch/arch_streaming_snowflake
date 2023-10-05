SELECT warehouse_name,
//:datebucket(end_time) query_end_date,
cluster_number ,
COUNT(*) queries, ROUND(AVG(total_elapsed_time/1000),2) as avg_query_time
,ROUND(MEDIAN(total_elapsed_time/1000),2) as "Median Query Time"
, (ROUND(AVG(execution_time/1000),0)) as avg_execution_time
, ROUND(MEDIAN(execution_time/1000),2) as "Median Execution Time"
, ROUND(MEDIAN(compilation_time/1000),2) as "Median Compilation Time"
, ROUND(MEDIAN(queued_overload_time/1000),0) as Median_Queued_Time
FROM SNOWFLAKE.account_usage.query_history
WHERE start_time BETWEEN '{start_date}' AND '{end_date}' --:daterange
and cluster_number is not null
and 
  warehouse_name IN ('{WAREHOUSE_NAME_IN}')
GROUP BY 1 , 2//,2
ORDER BY 1,2 //, 3;