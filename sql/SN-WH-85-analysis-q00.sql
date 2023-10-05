--create or replace transient table IDRC_SBX_IDROM.CMS_WORK_COMM_PRD.MONITOR_AND_ANALYS_WH AS -- {db_name}.{sc_name}
create or replace transient table {db_name}.{sc_name}.MONITOR_AND_ANALYS_WH AS
with credits as (
    select wmh.warehouse_name 
    ,      sum(credits_used) as credits_used
    from snowflake.account_usage.warehouse_metering_history wmh 
    where --wmh.start_time > dateadd(MONTH, -1, current_date()) -- FOR ONE month
    start_time BETWEEN '{start_date}' AND '{end_date}'
    group by wmh.warehouse_name
),
queries as ( 
	SELECT -- Queries over 1Gb in size
		qu.warehouse_name ,
		qu.warehouse_size ,
    CASE 
      WHEN ((Qu.TOTAL_ELAPSED_TIME) >= 0 and (Qu.TOTAL_ELAPSED_TIME) < 1000 * 60) THEN '1T1m'
      WHEN ((Qu.TOTAL_ELAPSED_TIME) >= 1000 * 60 and (Qu.TOTAL_ELAPSED_TIME) < 1000 * 60 * 5) THEN '2T1m-5m'
      WHEN ((Qu.TOTAL_ELAPSED_TIME) >= 1000 * 60 * 5 and (Qu.TOTAL_ELAPSED_TIME) < 1000 * 60 * 20) THEN '3T5m-20m'
      WHEN ((Qu.TOTAL_ELAPSED_TIME) >= 1000 * 60 * 20 and (Qu.TOTAL_ELAPSED_TIME) < 1000 * 60 * 60) THEN '4T20m-1H'
    ELSE '5TMORE 1H'
    END as Qs_PER_TIME,
    --COUNT(CASE WHEN ((Qu.TOTAL_ELAPSED_TIME) >= 0 and (Qu.TOTAL_ELAPSED_TIME) < 1000 * 60 *5)   THEN 1 ELSE NULL END) AS count_less5m ,
    --COUNT(CASE WHEN ((Qu.TOTAL_ELAPSED_TIME) >= 1000 * 60 *5 and (Qu.TOTAL_ELAPSED_TIME) < 1000 * 60 * 500)   THEN 1 ELSE NULL END) AS count_more5m ,
    
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
		--AND end_time > dateadd(MONTH,-1,CURRENT_DATE())
		and bytes_scanned > 0
        and TOTAL_ELAPSED_TIME > 0 
        and query_type in ('COPY', 'INSERT', 'MERGE', 'UNLOAD', 'RECLUSTER','SELECT','DELETE', 'CREATE_TABLE_AS_SELECT', 'UPDATE')
        --and (warehouse_name NOT like '{wh_selected}' and {exclude} = True)
        AND start_time BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY
        qu.warehouse_name,
        qu.warehouse_size, Qs_PER_TIME
)
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
    --count_less5m,
    --count_more5m,
    --ROUND(count_less5m / count_queries * 100, 0) AS percent_count_less5m ,
	--ROUND(count_more5m / count_queries * 100, 0) AS percent_count_more5m ,
    Qs_PER_TIME,
    is_local_spilling,
    is_remote_spilling,
    num_jobs_local_spilling,
    ROUND((num_jobs_local_spilling/ count_queries * 100),0) as perc_num_jobs_local_spilling,
    num_jobs_REMOTE_spilling,
    ROUND((num_jobs_REMOTE_spilling/ count_queries * 100),0) as perc_num_jobs_REMOTE_spilling,
    process_loaded,
    --UTILIZED_BLW50, UTILIZED_MR50,
    ROUND((cnt_UTILIZED_BLW50/ count_queries * 100),0) as perc_UTILIZED_BLW50,
    ROUND((cnt_UTILIZED_MR50/ count_queries * 100),0)  as perc_UTILIZED_MR50,
    
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
		ELSE (avg_large)
	END AS avg_bytes_large_gb ,
    CASE
		WHEN avg_bytes_scaned >= POWER(2, 40) THEN (ROUND(avg_bytes_scaned / POWER(1024, 3), 1)) 
		WHEN avg_bytes_scaned >= POWER(2, 30) THEN (ROUND(avg_bytes_scaned / POWER(1024, 3), 1)) 
		WHEN avg_bytes_scaned >= POWER(2, 20) THEN (ROUND(avg_bytes_scaned / POWER(1024, 3), 1)) 
		WHEN avg_bytes_scaned >= POWER(2, 10) THEN (ROUND(avg_bytes_scaned / POWER(1024, 3), 1)) 
		ELSE (avg_bytes_scaned)
	END AS avg_bytes_scaned_gb ,
    --ROUND(avg_bytes_scaned) as avg_bytes_scaned,
	ROUND(avg_large_exe_time) AS avg_large_exe_time ,
	ROUND(avg_execution_time) AS avg_all_exe_time ,
	count_queries,
    ROUND(c.credits_used) as credits_used
FROM queries q,
     credits c
WHERE q.warehouse_name = c.warehouse_name --and q.warehouse_size = c.warehouse_size 
        
--and perc_num_jobs_local_spilling > 0
--and q.warehouse_name NOT like 'OPIC_GLOBAL_TENANTS_WH'
--and Qs_PER_TIME <> '1T1m'
ORDER BY
   -- c.credits_used desc,
    wh_and_size desc, qs_per_time;
    ;
    ;
 --  Name:         SN-WH-scoring.sql  / SN-WH-85-analysis-q1.sql 
--  Created Date: 11-May-2023
--  Description:  
-- Warehouse 
------------------------------------------------------------------------------------------

-- Warehouse Analysis