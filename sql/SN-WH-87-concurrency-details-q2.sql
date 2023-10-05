
select --query_id, 
 warehouse_name,
 --TOTAL_ELAPSED_TIME/1000 as TOTAL_ELAPSED_TIME_s, QUEUED_OVERLOAD_TIME/1000 as QUEUED_OVERLOAD_TIME_s, QUERY_LOAD_PERCENT as QUERY_LOAD_PERCENT_1,
  CLUSTER_NUMBER
  ,TIME_SLICE(start_time::timestamp_ntz , 1, 'MINUTE') AS SLICE_FOR_1min
  --,* 
  ,count(*) as cnt
  --,SUM(cnt)   OVER (PARTITION BY warehouse_name, SLICE_FOR_1min ORDER BY SLICE_FOR_1min) as s_cnt
  from snowflake.account_usage.query_history where
  --start_time >= dateadd(day, 0, current_date() )
  start_time BETWEEN '{start_date}' AND '{end_date}'
  and 
 warehouse_name  IN ('{WAREHOUSE_NAME_IN}') 
 --('STARS_PRD_INFMT_3_WH')
--  ('STARS_PRD_INFMT_10_WH','STARS_PRD_INFMT_11_WH','STARS_PRD_INFMT_12_WH','STARS_PRD_INFMT_13_WH','STARS_PRD_INFMT_14_WH','STARS_PRD_INFMT_2_WH','STARS_PRD_INFMT_3_WH','STARS_PRD_INFMT_4_WH','STARS_PRD_INFMT_5_WH','STARS_PRD_INFMT_6_WH','STARS_PRD_INFMT_7_WH','STARS_PRD_INFMT_8_WH','STARS_PRD_INFMT_9_WH','STARS_PRD_INFMT_WH') --= $wh_name
  --warehouse_name like '%PRD_INFMT%'
  --and query_id = '01ad5264-0603-8542-0076-5103032af75a'
AND WAREHOUSE_SIZE IS NOT NULL
group by warehouse_name, --TOTAL_ELAPSED_TIME_s, QUEUED_OVERLOAD_TIME_s, QUERY_LOAD_PERCENT_1, 
CLUSTER_NUMBER, SLICE_FOR_1min
order by SLICE_FOR_1MIN, warehouse_name, CLUSTER_NUMBER
--)
--order by SLICE_FOR_1min, WAREHOUSE_NAME, CLUSTER_NUMBER --,S_CNT desc
 ;
 
/* 
select warehouse_name,
 CLUSTER_NUMBER
  ,TIME_SLICE(start_time::timestamp_ntz , 1, 'MINUTE') AS SLICE_FOR_1min--, QUERY_LOAD_PERCENT
  --,warehouse_size
  ,count(*) as cnt
  from snowflake.account_usage.query_history WHERE start_time BETWEEN '{start_date}' AND '{end_date}'
  and 
  warehouse_name = '{WAREHOUSE_NAME_IN}'
AND WAREHOUSE_SIZE IS NOT NULL
and query_type in ('COPY', 'UNLOAD', 'CREATE_TABLE', 'SELECT', 'UPDATE', 'MERGE', 'INSERT', 'CREATE_VIEW', 'CREATE_TABLE_AS_SELECT')
group by warehouse_name, CLUSTER_NUMBER,SLICE_FOR_1min
  order by warehouse_name, SLICE_FOR_1min, CLUSTER_NUMBER ; --CLUSTER_NUMBER desc
 ;
 */