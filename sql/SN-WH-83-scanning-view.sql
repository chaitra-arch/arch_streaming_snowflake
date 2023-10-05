--  Name:         SN-WH-scoring.sql  / SN-WH-scanning-view.sql 
--  Created Date: 11-May-2023
--  Description:  https://snowflakecomputing.atlassian.net/wiki/spaces/SKE/pages/2323415216/Health+check+of+Compute+-+Customer+Account
-- Warehouse scoring
------------------------------------------------------------------------------------------

-- Warehouse scoring last 30 days
with credits as (
    select wmh.warehouse_name 
    ,      sum(credits_used) as credits_used
    from snowflake.account_usage.warehouse_metering_history wmh 
    where --wmh.start_time > dateadd(month, -1, current_date()) -- FOR ONE month
	 start_time BETWEEN '{start_date}' AND '{end_date}'
    group by wmh.warehouse_name
), queries as ( 
	SELECT -- Queries over 1Gb in size
		qu.warehouse_name ,
		warehouse_size ,
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
		AND warehouse_size IS NOT NULL
		--AND end_time > dateadd(MONTH,-1,CURRENT_DATE())
		AND start_time BETWEEN '{start_date}' AND '{end_date}'
		and bytes_scanned > 0
		and TOTAL_ELAPSED_TIME > 0 
    GROUP BY
        qu.warehouse_name,
        warehouse_size
)
SELECT
   -- select --(WAREHOUSE_NAME || '-' || WAREHOUSE_SIZE) as WH_AND_SIZE ,
case warehouse_size
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
	ROUND(avg_large_exe_time) AS avg_large_exe_time ,
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
