
select --query_id, TOTAL_ELAPSED_TIME/1000 as TOTAL_ELAPSED_TIME_s, QUEUED_OVERLOAD_TIME/1000 as QUEUED_OVERLOAD_TIME_s, QUERY_LOAD_PERCENT as QUERY_LOAD_PERCENT_1,
  CLUSTER_NUMBER
  --,TIME_SLICE(start_time::timestamp_ntz , 1, 'MINUTE') AS SLICE_FOR_1min
  --,* 
  ,ROUND(avg(TOTAL_ELAPSED_TIME/1000),2) as TOTAL_ELAPSED_TIME_AVG
  from snowflake.account_usage.query_history 
  WHERE start_time BETWEEN '{start_date}' AND '{end_date}' --:daterange
and cluster_number is not null
and 
  warehouse_name IN ('{WAREHOUSE_NAME_IN}')
AND WAREHOUSE_SIZE IS NOT NULL
group by CLUSTER_NUMBER --query_id, TOTAL_ELAPSED_TIME_s, QUEUED_OVERLOAD_TIME_s, QUERY_LOAD_PERCENT_1, CLUSTER_NUMBER, SLICE_FOR_1min
  order by CLUSTER_NUMBER, TOTAL_ELAPSED_TIME_AVG desc
 ;