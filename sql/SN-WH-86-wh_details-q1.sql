select query_type, execution_status, --, user_name , --execution_status as execution_status1,
--(total_elapsed_time/1000) as execution_time_s,
ROUND(AVG(total_elapsed_time/1000),0) as AVG_total_elapsed_time_sec,
ROUND(MIN(total_elapsed_time/1000),2) as MIN_total_elapsed_time_sec,
ROUND(MIN(total_elapsed_time/1000/60),2) as MIN_total_elapsed_time_min,
ROUND(MAX(total_elapsed_time/1000),2) as MAX_total_elapsed_time_sec,
ROUND(MAX(total_elapsed_time/1000/60),2) as MAX_total_elapsed_time_min,

count(*)  as cnt_qury_type
from snowflake.account_usage.query_history
where warehouse_name = '{WAREHOUSE_NAME_IN}' --  'ACO_OS_PRD_PRVDR_BENE_WKLD'
and total_elapsed_time <= (1000 * 60 *60)
and start_time BETWEEN '{start_date}' AND '{end_date}' -->= dateadd(day, -30, current_date()) --and start_time <= dateadd(day, -30, current_date())
and execution_status = 'SUCCESS'-- 'FAIL'
and query_type in ('COPY', 'UNLOAD', 'CREATE_TABLE', 'SELECT', 'UPDATE', 'MERGE', 'INSERT', 'CREATE_VIEW', 'CREATE_TABLE_AS_SELECT') --('UNLOAD','SELECT')
group by query_type, execution_status--, user_name
order by cnt_qury_type desc, query_type 
;