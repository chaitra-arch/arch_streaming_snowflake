with job as
 (
select
    --QUERY_TEXT AS sql_text_hash,
    job.START_TIME AS created_on,
    TO_NUMBER(TOTAL_ELAPSED_TIME/1000,10,3) AS tot_sec, -- tot ~= misc+gs+q+xp
    TO_NUMBER(EXECUTION_TIME/1000,10,3) AS dur_xp_executing_sec,
    TO_NUMBER((QUEUED_OVERLOAD_TIME+QUEUED_PROVISIONING_TIME+QUEUED_REPAIR_TIME)/1000, 10,3) as dur_queue_sec,
    TO_NUMBER(TRANSACTION_BLOCKED_TIME / 1000, 10, 3) as dur_txn_lock_sec,
    TRANSACTION_BLOCKED_TIME + QUEUED_OVERLOAD_TIME+QUEUED_PROVISIONING_TIME+QUEUED_REPAIR_TIME+COMPILATION_TIME+LIST_EXTERNAL_FILES_TIME as pre_exe_ms,
    TO_NUMBER((TRANSACTION_BLOCKED_TIME + QUEUED_OVERLOAD_TIME+QUEUED_PROVISIONING_TIME+QUEUED_REPAIR_TIME+COMPILATION_TIME+LIST_EXTERNAL_FILES_TIME)/1000,10,3) as pre_exe_sec,
    dateadd(ms, pre_exe_ms, created_on) AS xp_start_time,
    dateadd(ms, pre_exe_ms + dur_xp_executing_sec, created_on) as xp_end_time,
    job.END_TIME as job_done_time,
    cluster_number as cluster_no,
    job.QUERY_ID AS uuid ,
    job.warehouse_id,
    job.warehouse_name,
    user_name,
     warehouse_size
 from  SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY job
 where WAREHOUSE_NAME = '{WAREHOUSE_NAME_IN}' --'IDRC_PRD_TRANSFORM_WH' --:Warehouse_Name
    and job.START_TIME >= dateadd(hour, -1 * 24, current_timestamp())
 )
 , time_in_ms as (
    select dateadd(ms, seq8(), dateadd(hour, -24, current_timestamp()))    as time_ms
    from table(generator(rowcount => 24 * 60 * 60 * 1000)) t --  2 hours
 )
, xp_job_count_sec as
(
   select date_trunc('minute', time_ms) as sec
     , cluster_no
     --, any_value(hash(sec::string, warehouse_name, cluster_no::string)) as key
     , avg(job_active_count) as avg_job_active_count_ms
     , max(job_active_count) as max_job_active_count_ms
   from
   (
        select time_ms
         , cluster_no
         , count(distinct uuid) as job_active_count
         FROM time_in_ms t  -- roll up to ms level
         --left join job
         inner join job
         on time_ms >= xp_start_time and time_ms < xp_end_time
         group by 1, 2
    )
    group by 1, 2
    order by 1, 2
 )
 
  select * from
  (
   SELECT convert_timezone('America/Los_Angeles',x.sec) as time_sec
   , case when x.cluster_no > 0 then 'cluster' || x.cluster_no::varchar(2) 
            else 'None'end as cluster_number
   , avg_job_active_count_ms
  , max_job_active_count_ms
      ,x.cluster_no
 from xp_job_count_sec x
 
 )
 where cluster_number <> 'None'
 order by time_sec, cluster_number
;