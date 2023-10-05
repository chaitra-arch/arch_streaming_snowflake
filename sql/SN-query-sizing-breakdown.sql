--  Name:         SN-WH-Query-Sizing-Breakdown.sql  
--  Created Date: 24-April-2023
--  Description:  Took one of the dashboard queries and put it in here to see if it would work in CAS2
------------------------------------------------------------------------------------------
// WH Query Sizing Breakdown - Percent of Queries
WITH credits as (
    select wmh.warehouse_name 
    ,      sum(credits_used_compute) as credits_used
    from snowflake.account_usage.warehouse_metering_history wmh 
    where wmh.start_time BETWEEN '{start_date}' AND '{end_date}'
    group by wmh.warehouse_name
  ), queries as (
  SELECT
  warehouse_name ,
  warehouse_size,
  AVG(avg_10GB_plus) AS avg_10GB_plus,
  COUNT(count_10GB_plus) as count_10GB_plus,
  COUNT(count_1_to_10GB) AS count_1_to_10GB,
COUNT(count_lt_half_GB) AS count_lt_half_GB,
    COUNT(count_half_to_1GB) AS count_half_to_1GB,
  AVG(avg_10GB_plus_exe_time) AS avg_10GB_plus_exe_time,
    AVG(avg_1_to_10GB_exe_time) AS avg_1_to_10GB_exe_time,
  AVG(avg_bytes_scanned) AS avg_bytes_scanned,
  AVG(avg_elapsed_time) AS avg_elapsed_time,
  AVG(avg_execution_time) AS avg_execution_time,
  COUNT(*) AS count_queries
    FROM
                (
      SELECT -- Queries over 1Gb in size
                                qu.warehouse_name ,
                                warehouse_size ,
                 CASE WHEN bytes_scanned   >= 10000000000  THEN bytes_scanned ELSE NULL END AS avg_10GB_plus,
                CASE WHEN bytes_scanned >= 10000000000  THEN 1 ELSE NULL END AS count_10GB_plus,       
                 CASE WHEN bytes_scanned BETWEEN 1000000000 AND 10000000000 THEN 1 ELSE NULL END AS count_1_to_10GB ,
     CASE WHEN bytes_scanned BETWEEN 1000000000 AND 10000000000 THEN execution_time / 1000 ELSE NULL END AS avg_1_to_10GB_exe_time, 
     CASE WHEN bytes_scanned   >= 10000000000  THEN execution_time / 1000 ELSE NULL END AS avg_10GB_plus_exe_time ,
        CASE WHEN bytes_scanned BETWEEN 500000000 AND 1000000000  THEN 1 ELSE NULL END AS count_half_to_1GB, 
        CASE WHEN bytes_scanned < 500000000  THEN 1 ELSE NULL END AS count_lt_half_GB, 
                    bytes_scanned AS avg_bytes_scanned ,
                    total_elapsed_time/ 1000 AS avg_elapsed_time ,
    execution_time/ 1000 AS avg_execution_time
                FROM
                                snowflake.account_usage.query_history qu
                WHERE
                                execution_status = 'SUCCESS'
                                AND warehouse_size IS NOT NULL
                                AND end_time > dateadd(WEEK,-1,CURRENT_DATE())
                                and bytes_scanned > 0
                                and TOTAL_ELAPSED_TIME > 0 
        --and warehouse_name = :warehouse_name
  )
    GROUP BY
        warehouse_name,
        warehouse_size
)
SELECT
                q.warehouse_name , -- Warehouse Name
                q.warehouse_size ,
                ROUND(count_10GB_plus / count_queries * 100, 0) AS "PERCENT_10GB_PLUS" ,
                ROUND(count_1_to_10GB / count_queries * 100, 0) AS "PERCENT_1_TO_10GB" ,
    ROUND(count_lt_half_GB / count_queries * 100, 0) AS "PERCENT_LESS_500MB" ,
    ROUND(count_half_to_1GB / count_queries * 100, 0) AS "PERCENT_500MB_TO_1GB" ,
    ROUND(CREDITS_USED/COUNT_QUERIES, 2) as AVG_QUERY_COST,
    --count_half_to_1GB
    --count_lt_1GB
                /*CASE
                                WHEN avg_large >= POWER(2, 40) THEN to_char(ROUND(avg_large / POWER(2, 40), 1)) || ' TB'
                                WHEN avg_large >= POWER(2, 30) THEN to_char(ROUND(avg_large / POWER(2, 30), 1)) || ' GB'
                                WHEN avg_large >= POWER(2, 20) THEN to_char(ROUND(avg_large / POWER(2, 20), 1)) || ' MB'
                                WHEN avg_large >= POWER(2, 10) THEN to_char(ROUND(avg_large / POWER(2, 10), 1)) || ' K'
                                ELSE to_char(avg_large)
                END AS avg_bytes_large ,
    */
                ROUND(avg_10GB_plus_exe_time) AS avg_10GB_plus_exe_time ,
                ROUND(avg_execution_time) AS avg_all_exe_time ,
                count_queries,
    ROUND(c.credits_used) as credits_used
FROM queries q,
     credits c
WHERE q.warehouse_name = c.warehouse_name
ORDER BY
    c.credits_used desc,
    case warehouse_size
       when 'X-Small' then 1
       when 'Small'   then 2
       when 'Medium'  then 3
       when 'Large'   then 4
       when 'X-Large' then 5
       when '2X-Large' then 6
       when '3X-Large' then 7
       when '4X-Large' then 8
       else 9
       end desc;