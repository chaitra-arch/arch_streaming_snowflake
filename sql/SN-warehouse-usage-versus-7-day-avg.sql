--  Name:         SN-warehouse-usage-versus-7-day-avg.sql  
--  Created Date: 24-April-2023
--  Description:  Took one of the dashboard queries and put it in here to see if it would work in CAS2
------------------------------------------------------------------------------------------
SELECT WAREHOUSE_NAME,
       DATE(START_TIME) AS DATE,
       SUM(CREDITS_USED) AS CREDITS_USED,
       AVG(SUM(CREDITS_USED)) OVER (
          PARTITION BY WAREHOUSE_NAME
          ORDER BY
             DATE ROWS 7 PRECEDING
       ) AS CREDITS_USED_7_DAY_AVG,
       (TO_NUMERIC(SUM(CREDITS_USED)/CREDITS_USED_7_DAY_AVG*100,10,2)-100)::STRING || '%' AS VARIANCE_TO_7_DAY_AVERAGE
  FROM "SNOWFLAKE"."ACCOUNT_USAGE"."WAREHOUSE_METERING_HISTORY"
 WHERE START_TIME > current_date() -7
 GROUP BY DATE, WAREHOUSE_NAME
 ORDER BY VARIANCE_TO_7_DAY_AVERAGE DESC;