--  Name:         SN-query-queue-details.sql  
--  Created Date: 16-May-2023
--  Description:  
------------------------------------------------------------------------------------------
select (warehouse_name || '-' || warehouse_size ) as wh_and_size, 
CASE 
    WHEN QUERY_LOAD_PERCENT < 25 THEN 'LT25' 
    WHEN QUERY_LOAD_PERCENT < 50 THEN 'GT25' 
    WHEN QUERY_LOAD_PERCENT < 75 THEN 'GT50' 
    ELSE 'GT75' END workload_GR, 
(CLUSTER_NUMBER), COUNT(*) as CNT
from snowflake.account_usage.query_history 
where warehouse_name =  '{option_wh}'--
AND WAREHOUSE_SIZE IS NOT NULL
AND start_time BETWEEN  '{start_date}' AND '{end_date}'
--and start_time::date >= '2023-05-14'
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
;