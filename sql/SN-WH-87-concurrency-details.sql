
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