--  Name:         SN-warehouse-load-history.sql  
--  Created Date: 24-April-2023
--  Description:  Took one of the dashboard queries and put it in here to see if it would work in CAS2
------------------------------------------------------------------------------------------
SELECT convert_timezone('America/Chicago', H.start_time::timestamp_ntz) AS START_TIME 
       ,H.WAREHOUSE_NAME
       ,ROUND(H.avg_running,1) AS AVG_RUNNING
       ,ROUND(H.avg_queued_load,1) AS AVG_QUEUED_LOAD 
       ,ROUND(H.avg_blocked,1) AS AVG_BLOCKED 
  FROM snowflake.account_usage.warehouse_load_history AS H
 INNER JOIN (SELECT TOP 10
       WAREHOUSE_NAME, 
       SUM(ROUND(avg_queued_load)) TOTAL_QUEUED_LOAD
  FROM snowflake.account_usage.warehouse_load_history
 GROUP BY WAREHOUSE_NAME
 ORDER BY TOTAL_QUEUED_LOAD) AS T
    ON H.WAREHOUSE_NAME = T.WAREHOUSE_NAME
WHERE TO_DATE(H.START_TIME) BETWEEN '{start_date}' AND '{end_date}'
ORDER BY H.START_TIME