
--- STEP 4

WITH
CTE_QH AS (
    SELECT START_TIME
          ,END_TIME
          ,TOTAL_ELAPSED_TIME - (LIST_EXTERNAL_FILES_TIME+COMPILATION_TIME+QUEUED_PROVISIONING_TIME+QUEUED_REPAIR_TIME+QUEUED_OVERLOAD_TIME+TRANSACTION_BLOCKED_TIME) AS WH_TIME
          ,DATEADD(MILLISECOND, -WH_TIME, END_TIME) AS START_TIME_WH
          ,WAREHOUSE_ID
          ,WAREHOUSE_NAME
          ,WAREHOUSE_SIZE
          ,WAREHOUSE_TYPE
          ,CLUSTER_NUMBER
          ,QUERY_TAG
          ,SESSION_ID
          ,USER_NAME
          ,ROLE_NAME
          ,DATABASE_ID
          ,DATABASE_NAME
          ,SCHEMA_ID
          ,SCHEMA_NAME
          ,QUERY_TYPE
          ,EXECUTION_STATUS
          ,ERROR_CODE
          ,ERROR_MESSAGE
      FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
     WHERE CLUSTER_NUMBER IS NOT NULL
        AND execution_status = 'SUCCESS'
		AND warehouse_size IS NOT NULL
		AND end_time  BETWEEN'{start_date}' AND '{end_date}' -- end_time > dateadd(MONTH,-1,CURRENT_DATE())
        and TOTAL_ELAPSED_TIME > 0 
        and query_type in ('COPY', 'INSERT', 'MERGE', 'UNLOAD', 'RECLUSTER','SELECT','DELETE', 'CREATE_TABLE_AS_SELECT', 'UPDATE')
         AND WAREHOUSE_SIZE IS NOT NULL
         AND WAREHOUSE_SIZE NOT IN ('X-Small', 'Small', 'Medium') -- Discarding 'LIGHT' WH Sizes
       --AND START_TIME >= DATEADD(HOUR, -1, $START_TIME_MIN)
       --AND START_TIME < $START_TIME_MAX
       
       --AND warehouse_name = $WAREHOUSE_NAME_IN --and warehouse_size = 'X-Large'
       AND TOTAL_ELAPSED_TIME <= (1000 * 60) -- 1 minute or less // 20: 5913 // 5: 5906 // 1m: 5781
  )
  ,
  CTE_WMH AS (
    SELECT --*,
            START_TIME
          ,END_TIME
          ,WAREHOUSE_ID
          ,WAREHOUSE_NAME
          ,CREDITS_USED_COMPUTE
      FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
     WHERE  end_time BETWEEN'{start_date}' AND '{end_date}' -- >=  dateadd(MONTH,-1,CURRENT_DATE()) -- BETWEEN'{start_date}' AND '{end_date}'--
    --START_TIME >= $START_TIME_MIN AND START_TIME < $START_TIME_MAX
       --AND warehouse_name = $WAREHOUSE_NAME_IN --and warehouse_size = 'X-Large'
  )
  ,CTE_OVERLAP AS (
    SELECT WMH.WAREHOUSE_ID
          ,WMH.WAREHOUSE_NAME
          ,GREATEST(WMH.START_TIME, IFNULL(QH.START_TIME_WH, WMH.START_TIME)) AS START_TIME_SEGMENT
          ,LEAST(WMH.END_TIME, IFNULL(QH.END_TIME, WMH.END_TIME)) AS END_TIME_SEGMENT
          ,WMH.CREDITS_USED_COMPUTE * 
             (
               DATEDIFF(MILLISECOND, START_TIME_SEGMENT, END_TIME_SEGMENT) / 
               SUM(DATEDIFF(MILLISECOND, START_TIME_SEGMENT, END_TIME_SEGMENT)) OVER (PARTITION BY WMH.WAREHOUSE_ID, WMH.START_TIME)
             ) AS SEGMENT_CREDITS
          ,QH.WAREHOUSE_SIZE
          ,QH.WAREHOUSE_TYPE
          ,QH.CLUSTER_NUMBER
          ,QH.QUERY_TAG
          ,QH.SESSION_ID
          ,QH.USER_NAME
          ,QH.ROLE_NAME
          ,QH.DATABASE_ID
          ,QH.DATABASE_NAME
          ,QH.SCHEMA_ID
          ,QH.SCHEMA_NAME
          ,QH.QUERY_TYPE
          ,QH.EXECUTION_STATUS
          ,QH.ERROR_CODE
          ,QH.ERROR_MESSAGE
      FROM CTE_WMH WMH
           LEFT JOIN CTE_QH QH
             ON WMH.WAREHOUSE_ID = QH.WAREHOUSE_ID
            AND WMH.START_TIME < QH.END_TIME
            AND QH.START_TIME_WH < WMH.END_TIME
  ),CTE_CREDIT_RESULT AS
  (
  SELECT WAREHOUSE_NAME, WAREHOUSE_SIZE, --CLUSTER_NUMBER, 
  SUM(SEGMENT_CREDITS) /2 as SAVING_CREDITS_SIZE_DOWN_1minBKT -- Going Down by ONE Size
  --*
    FROM CTE_OVERLAP
    WHERE WAREHOUSE_SIZE IS NOT NULL
    GROUP BY WAREHOUSE_NAME, WAREHOUSE_SIZE--, CLUSTER_NUMBER
    HAVING SAVING_CREDITS_SIZE_DOWN_1minBKT > 0
    --ORDER BY SAVING_CREDITS_SIZE_DOWN_1minBKT DESC, WAREHOUSE_NAME 
)

-- COST Part Ends
SELECT wh_and_size, warehouse_size, --SAVING_CREDITS_SIZE_DOWN_1minBKT, 
TOTAL_SAVING_CREDITS_SIZE_DOWN_1minBKT as POSSIBLE_SAVING_CREDITS_ON_1MIN_BKT
--,CALC_SAVING
FROM (
select mq.wh_and_size, --WAREHOUSE_NAME, warehouse_size,
mq.warehouse_size,
ROUND(min(SAVING_CREDITS_SIZE_DOWN_1minBKT), 2) as SAVING_CREDITS_SIZE_DOWN_1minBKT,
ROUND(SUM(SAVING_CREDITS_SIZE_DOWN_1minBKT), 2) as TOTAL_SAVING_CREDITS_SIZE_DOWN_1minBKT,
count(*) as cnt_recommendations,
REPLACE(
REPLACE(
to_char(ARRAY_AGG(TRIM(RULE_OUTPUT1)) WITHIN GROUP (ORDER BY mq.WAREHOUSE_NAME ASC)),'[','') 
,']',''
) as recommendations,
--TRIM(RULE_OUTPUT1) as RECOMMEND_OUTPUT --, QS_PER_TIME as QS_PER_TIME1
--, * 
CASE 
    WHEN (recommendations LIKE '%0.%' or recommendations LIKE '%1.%' or recommendations LIKE '%2.%') THEN 1 ELSE 0
END as CALC_SAVING
--,AVG(SAVING_CREDITS_SIZE_DOWN_1minBKT * CALC_SAVING) as POSSIBLE_SAVING
FROM 
(--------
    select * from  {db_name}.{sc_name}.MONITOR_AND_ANALYS_WH_STEP_TWO --IDRC_SBX_IDROM.CMS_WORK_COMM_PRD.MONITOR_AND_ANALYS_WH_STEP_TWO
) mq
JOIN CTE_CREDIT_RESULT cr ON (mq.warehouse_name = cr.warehouse_name AND mq.warehouse_size = cr.warehouse_size)
WHERE TRIM(RULE_OUTPUT1) <> '' 
--and warehouse_name like '%ACO%';
group by mq.wh_and_size, mq.warehouse_size --,warehouse_size 
HAVING  CALC_SAVING = 1
)
order by POSSIBLE_SAVING_CREDITS_ON_1MIN_BKT DESC, wh_and_size asc;

/* 
dits as (
    select wmh.warehouse_name 
    ,      sum(credits_used) as credits_used
    from snowflake.account_usage.warehouse_metering_history wmh 
    where wmh.start_time BETWEEN '{start_date}' AND '{end_date}'-->-- FOR ONE month
    group by wmh.warehouse_name
), queries as ( 
	SELECT -- Queries over 1Gb in size
		qu.warehouse_name ,
		qu.warehouse_size ,
    CASE 
      WHEN ((Qu.TOTAL_ELAPSED_TIME) >= 0 and (Qu.TOTAL_ELAPSED_TIME) < 1000 * 60) THEN '1T1m'
      WHEN ((Qu.TOTAL_ELAPSED_TIME) >= 1000 * 60 and (Qu.TOTAL_ELAPSED_TIME) < 1000 * 60 * 5) THEN '2T1m-5m'
      WHEN ((Qu.TOTAL_ELAPSED_TIME) >= 1000 * 60 * 5 and (Qu.TOTAL_ELAPSED_TIME) < 1000 * 60 * 20) THEN '3T5m-20m'
      WHEN ((Qu.TOTAL_ELAPSED_TIME) >= 1000 * 60 * 20 and (Qu.TOTAL_ELAPSED_TIME) < 1000 * 60 * 60) THEN '4T20m-1H'
    ELSE '5TMORE-1H'
    END as Qs_PER_TIME,
    
    sum(case when bytes_spilled_to_local_storage > 0 then 1 else 0 end) as num_jobs_local_spilling,
    MAX(iff(BYTES_SPILLED_TO_LOCAL_STORAGE > 0, 1,0)) as is_local_spilling,
    sum(case when bytes_spilled_to_remote_storage > 0 then 1 else 0 end) as num_jobs_remote_spilling,
    MAX(iff(BYTES_SPILLED_TO_remote_STORAGE >0, 1,0)) as is_remote_spilling,
    sum(CASE WHEN QUERY_LOAD_PERCENT > 0 and QUERY_LOAD_PERCENT < 50 THEN 1 ELSE 0 END) as cnt_UTILIZED_BLW50, -- WHEN QUERY_LOAD_PERCENT >= 50 and QUERY_LOAD_PERCENT <= 100 THEN 'UTILIZED_MR50' 
    sum(CASE WHEN QUERY_LOAD_PERCENT >= 50 and QUERY_LOAD_PERCENT <= 100 THEN 1 ELSE 0  END) as cnt_UTILIZED_MR50, 
    //sum(case when bytes_spilled_to_remote_storage > 0 then 1 else 0 end)  as pct_jobs_spilled_remote ,
    ROUND(AVG(QUERY_LOAD_PERCENT)) as process_loaded,
    
    --count(*) as CNT ,
    AVG(bytes_scanned) AS avg_bytes_scaned ,
		AVG(CASE WHEN bytes_scanned   >= 1000000000  THEN bytes_scanned ELSE NULL END) AS avg_large ,
		COUNT(CASE WHEN bytes_scanned >= 1000000000  THEN 1 ELSE NULL END) AS count_large ,
		COUNT(CASE WHEN bytes_scanned <  1000000000  THEN 1 ELSE NULL END) AS count_small ,
		AVG(CASE WHEN bytes_scanned   >= 1000000000  THEN total_elapsed_time / 1000 ELSE NULL END) AS avg_large_exe_time ,
		AVG(bytes_scanned) AS avg_bytes_scanned ,
		AVG(total_elapsed_time)/ 1000 AS avg_elapsed_time ,
		AVG(execution_time)/ 1000 AS avg_execution_time ,
		COUNT(*) AS count_queries
  
	FROM
		snowflake.account_usage.query_history qu
	WHERE
		execution_status = 'SUCCESS'
		AND qu.warehouse_size IS NOT NULL
		AND end_time  BETWEEN'{start_date}' AND '{end_date}'-->
		and bytes_scanned > 0
        and TOTAL_ELAPSED_TIME > 0 
        and query_type in ('COPY', 'INSERT', 'MERGE', 'UNLOAD', 'RECLUSTER','SELECT','DELETE', 'CREATE_TABLE_AS_SELECT', 'UPDATE')
    GROUP BY
        qu.warehouse_name,
        qu.warehouse_size, Qs_PER_TIME
), main_q as
(
SELECT
   -- select --(WAREHOUSE_NAME || '-' || WAREHOUSE_SIZE) as WH_AND_SIZE ,
case q.warehouse_size
when 'X-Small' then 1
when 'Small' then 2
when 'Medium' then 3
when 'Large' then 4
when 'X-Large' then 5
when '2X-Large' then 6
when '3X-Large' then 7
when '4X-Large' then 8
else 9
end 
|| '-' || (q.WAREHOUSE_NAME || '-' || q.WAREHOUSE_SIZE) as WH_AND_SIZE ,
	q.warehouse_name , -- Warehouse Name
	q.warehouse_size ,
	ROUND(count_large / count_queries * 100, 0) AS percent_large ,
	ROUND(count_small / count_queries * 100, 0) AS percent_small ,
    num_jobs_local_spilling,
    num_jobs_remote_spilling,
    ROUND((num_jobs_local_spilling/ count_queries * 100),0) as per_num_jobs_local_spilling,
    ROUND((num_jobs_REMOTE_spilling/ count_queries * 100),0) as per_num_jobs_REMOTE_spilling,
    Qs_PER_TIME,
    is_local_spilling,
    is_remote_spilling,
    process_loaded,
    ROUND((cnt_UTILIZED_BLW50/ count_queries * 100),0) as perc_UTILIZED_BLW50,
    ROUND((cnt_UTILIZED_MR50/ count_queries * 100),0) as perc_UTILIZED_MR50,
	CASE
		WHEN avg_large >= POWER(2, 40) THEN to_char(ROUND(avg_large / POWER(2, 40), 1)) || ' TB'
		WHEN avg_large >= POWER(2, 30) THEN to_char(ROUND(avg_large / POWER(2, 30), 1)) || ' GB'
		WHEN avg_large >= POWER(2, 20) THEN to_char(ROUND(avg_large / POWER(2, 20), 1)) || ' MB'
		WHEN avg_large >= POWER(2, 10) THEN to_char(ROUND(avg_large / POWER(2, 10), 1)) || ' K'
		ELSE to_char(avg_large)
	END AS avg_bytes_large ,
    CASE
		WHEN avg_large >= POWER(2, 40) THEN (ROUND(avg_large / POWER(1024, 3), 1)) 
		WHEN avg_large >= POWER(2, 30) THEN (ROUND(avg_large / POWER(1024, 3), 1)) 
		WHEN avg_large >= POWER(2, 20) THEN (ROUND(avg_large / POWER(1024, 3), 1)) 
		WHEN avg_large >= POWER(2, 10) THEN (ROUND(avg_large / POWER(1024, 3), 1)) 
		ELSE to_char(avg_large)
	END AS avg_bytes_large_gb ,
    CASE
		WHEN avg_bytes_scaned >= POWER(2, 40) THEN (ROUND(avg_bytes_scaned / POWER(1024, 3), 1)) 
		WHEN avg_bytes_scaned >= POWER(2, 30) THEN (ROUND(avg_bytes_scaned / POWER(1024, 3), 1)) 
		WHEN avg_bytes_scaned >= POWER(2, 20) THEN (ROUND(avg_bytes_scaned / POWER(1024, 3), 1)) 
		WHEN avg_bytes_scaned >= POWER(2, 10) THEN (ROUND(avg_bytes_scaned / POWER(1024, 3), 1)) 
		ELSE (avg_bytes_scaned)
	END AS avg_bytes_scaned_gb ,
	ROUND(avg_large_exe_time) AS avg_large_exe_time ,
	ROUND(avg_execution_time) AS avg_all_exe_time ,
	count_queries,
    ROUND(c.credits_used) as credits_used
FROM queries q,
     credits c
        
WHERE q.warehouse_name = c.warehouse_name --and q.warehouse_size = c.warehouse_size
--and q.warehouse_name not like 'IDRC_PRD_TRANSFORM_WH'
)

select wh_and_size, warehouse_size, count(*) as cnt_recommendations,
REPLACE(
REPLACE(
to_char(ARRAY_AGG(TRIM(RULE_OUTPUT1)) WITHIN GROUP (ORDER BY WAREHOUSE_NAME ASC)),'[','') 
,']',''
) as recommendations
--TRIM(RULE_OUTPUT1) as RECOMMEND_OUTPUT --, QS_PER_TIME as QS_PER_TIME1
--, * 

from (
select WAREHOUSE_NAME as WAREHOUSE_NAME1, COUNT_QUERIES as COUNT_QUERIES2, --, select max(COUNT_QUERIES) as m_COUNT_QUERIES from from table(result_scan('01aca345-0000-43db-0000-041d44ed7a66')),
(NUM_JOBS_REMOTE_SPILLING/ COUNT_QUERIES) * 100 as perc_ratio_remote_spilling,
(NUM_JOBS_LOCAL_SPILLING/ COUNT_QUERIES) * 100 as perc_ratio_local_spilling,
--STDDEV(COUNT_QUERIES) std_div,
AVG(COUNT_QUERIES) OVER(PARTITION BY warehouse_name, WAREHOUSE_SIZE) avg_cnt_q_per_wh, 
COUNT(COUNT_QUERIES) OVER(PARTITION BY warehouse_name, WAREHOUSE_SIZE) count_bukts_q_per_wh, 
SUM(COUNT_QUERIES) OVER(PARTITION BY warehouse_name) sum_cnt_q_per_wh, 
ROUND((COUNT_QUERIES / avg_cnt_q_per_wh), 1) as q_cnt_by_avg_qs,
round((COUNT_QUERIES / SUM_CNT_Q_PER_WH), 4) num_qs_in_bucket,
-- Check if only Qs in SMALL range for SIZE more MEDIUM; 1 and 1 to 5 mins EXECUTIONS only
    CASE 
    WHEN (QS_PER_TIME LIKE '1T%' or QS_PER_TIME LIKE '2T%') and NUM_QS_IN_BUCKET > 0.9 -- 90% for 1 min Qs
    --and Q_CNT_BY_AVG_QS >= 1 
    and WAREHOUSE_SIZE NOT IN ('X-Small', 'Small') 
    AND COUNT_BUKTS_Q_PER_WH <= 2
    AND PERCENT_SMALL > PERCENT_LARGE AND PROCESS_LOADED < 90
    --and PERC_UTILIZED_BLW50 < 50
    
    THEN '1. Downsize WH->-WH running ONLY small queries less then 1 minute. WH bigger than MEDIUM Size.' ELSE '' -- SIZE Down WH
    END || '#' ||
-------------------------------- 
---- 
-- ANY TIME BUcket; Check if very few Qs are in heavy Executoins; 
    CASE 
    WHEN --(QS_PER_TIME LIKE '1T%' or QS_PER_TIME LIKE '2T%') and 
    Q_CNT_BY_AVG_QS <= 0.1 --NUM_QS_IN_BUCKET <= 0.0015 -- 90% for 1 min Qs
    --and Q_CNT_BY_AVG_QS >= 1 
    and WAREHOUSE_SIZE NOT IN ('X-Small', 'Small') 
    --AND COUNT_BUKTS_Q_PER_WH <= 2
    AND PERCENT_SMALL < PERCENT_LARGE AND PROCESS_LOADED > 90
    --and PERC_UTILIZED_BLW50 < 50
    
    THEN '2. Split LIGHT workload->-WH running a VERY few queries in the TIME_BUCKET:' || QS_PER_TIME ELSE '' -- SIZE Down WH
    END || '#' ||
-------------------------------- 
---- 
-- Remote Spilling; 
    CASE 
    WHEN --(QS_PER_TIME LIKE '1T%' or QS_PER_TIME LIKE '2T%') and 
    PERC_RATIO_REMOTE_SPILLING >= 0.1 --NUM_QS_IN_BUCKET <= 0.0015 -- 90% for 1 min Qs
    --and Q_CNT_BY_AVG_QS >= 1 
    --and WAREHOUSE_SIZE NOT IN ('X-Small', 'Small', 'Medium') 
    --AND COUNT_BUKTS_Q_PER_WH <= 2
    --AND PERCENT_SMALL < PERCENT_LARGE AND PROCESS_LOADED > 90
    --and PERC_UTILIZED_BLW50 < 50
    
    THEN '3. Split workload->WH having a REMOTE spilling TIME_BUCKET:' || QS_PER_TIME ELSE '' -- SIZE Down WH
    END || '#' ||
-------------------------------- 
----
 CASE 
    WHEN --(QS_PER_TIME LIKE '1T%' or QS_PER_TIME LIKE '2T%') and 
    NUM_QS_IN_BUCKET > 0.9 -- 90% for 1 min Qs
    --and Q_CNT_BY_AVG_QS >= 1 
    and WAREHOUSE_SIZE NOT IN ('X-Small', 'Small') 
    --AND COUNT_BUKTS_Q_PER_WH <= 2
    AND PERCENT_SMALL > PERCENT_LARGE AND PROCESS_LOADED < 90
    --and PERC_UTILIZED_BLW50 < 50

THEN '5. Split LIGHT workload or Downsize WH -> WH running many queries less then 1 minute. Split LIGHT workload' ELSE '' -- SIZE Down WH
END || '#' ||


CASE
WHEN COUNT_BUKTS_Q_PER_WH = 5 and QS_PER_TIME LIKE '5T%' and WAREHOUSE_SIZE NOT IN ('X-Small', 'Small', 'Medium') 

THEN '6. Check for NUM of: ' || COUNT_QUERIES || ' - Queries Running time more 1 HOUR for the WH:' || WAREHOUSE_NAME  

ELSE ''
END --PER_TIME LIKE '1T%' and PERC_RATIO_LOCAL_SPILLING > 3 THEN 'Small LOCAL spilling. Split to BIGGER size WH'  -- 5% Spilling ration on Bucket1
--END 
rule_output,
replace(rule_output, '#', ' ') as rule_output1,
* 
from main_q q1
)

where 
TRIM(RULE_OUTPUT1) <> '' 
--and warehouse_name like '%ACO%';
group by wh_and_size, warehouse_size 
order by wh_and_size asc;

 */
 --  Name:         SN-WH-scoring.sql  / SN-WH-85-analysis-q1.sql 
--  Created Date: 11-May-2023
--  Description:  
-- Warehouse 
------------------------------------------------------------------------------------------

-- Warehouse Analysis