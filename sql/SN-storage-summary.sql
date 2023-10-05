--  Name:         SN-storage-summary.sql  
--  Created Date: 24-April-2023
--  Description:  Took one of the dashboard queries and put it in here to see if it would work in CAS2
------------------------------------------------------------------------------------------
select date_trunc(month, usage_date) as usage_month,
       avg(storage_bytes + stage_bytes + failsafe_bytes) / power(1024, 4) as billable_tb,
       avg(storage_bytes) / power(1024, 4) as Storage_TB,
       avg(stage_bytes) / power(1024, 4) as Stage_TB,
       avg(failsafe_bytes) / power(1024, 4) as Failsafe_TB
  from snowflake.account_usage.storage_usage
 WHERE date(usage_month) BETWEEN '{start_date}' AND '{end_date}'
 group by 1
 order by 1 desc;