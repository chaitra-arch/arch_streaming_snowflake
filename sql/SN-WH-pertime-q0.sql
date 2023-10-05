--  Name:         SN-WH-scoring.sql  / SN-WH-pretime-q0.sql 
--  Created Date: 11-May-2023
--  Description:  https://snowflakecomputing.atlassian.net/wiki/spaces/SKE/pages/2323415216/Health+check+of+Compute+-+Customer+Account
-- Warehouse time split 0 to 5m / 5m to +++; 
------------------------------------------------------------------------------------------

-- Warehouse scoring last 7 /// 30 days
SELECT 
(warehouse_name || '-' || warehouse_size) as WH_AND_SIZE,
warehouse_name, warehouse_size, --QUERY_TYPE,
CASE 
    --WHEN ((Q.TOTAL_ELAPSED_TIME) >= 0 and (Q.TOTAL_ELAPSED_TIME) < 1000 * 60) THEN '1T1m'
    WHEN ((Q.TOTAL_ELAPSED_TIME) > 0 and (Q.TOTAL_ELAPSED_TIME) < 1000 * 60 * 5) THEN '1T0m-5m'
    --WHEN ((Q.TOTAL_ELAPSED_TIME) >= 1000 * 60 * 5 and (Q.TOTAL_ELAPSED_TIME) < 1000 * 60 * 20) THEN '35m-20m'
    WHEN ((Q.TOTAL_ELAPSED_TIME) >= 1000 * 60 * 5 and (Q.TOTAL_ELAPSED_TIME) < 1000 * 60 * 120) THEN '2T5m-2H'
    WHEN ((Q.TOTAL_ELAPSED_TIME) >= 1000 * 60 * 120 ) THEN '3T2H+'
    ELSE '4MORE 4H'
END as Qs_PER_TIME,
ROUND(AVG(QUERY_LOAD_PERCENT)) as process_loaded,
ROUND(MIN(QUERY_LOAD_PERCENT)) as process_loaded_min,
ROUND(MAX(QUERY_LOAD_PERCENT)) as process_loaded_max,
count(*) as CNT   
FROM  SNOWFLAKE.account_usage.query_history q
WHERE TO_DATE(Q.START_TIME)  BETWEEN'{start_date}' AND '{end_date}' --BETWEEN DATEADD(day,-1,TO_DATE(current_date())) AND TO_DATE(current_date()) 
--and warehouse_name = $WAREHOUSE_NAME
and (WAREHOUSE_NAME not like 'COMPUTE_SERVICE%')
and WAREHOUSE_SIZE is not NULL
and TOTAL_ELAPSED_TIME > 0 
GROUP BY 1,2,3,4
order by 1,3,2
          ;
