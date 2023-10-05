--  Name:         SN-credits-billed-monthly.sql  
--  Created Date: 24-April-2023
--  Description:  Took one of the dashboard queries and put it in here to see if it would work in CAS2
------------------------------------------------------------------------------------------
select date_trunc('MONTH', usage_date) as Usage_Month,
       sum(CREDITS_BILLED) as Credits_Billed
  from snowflake.account_usage.metering_daily_history
 WHERE date(usage_month) BETWEEN '{start_date}' AND '{end_date}'
 group by Usage_Month
 order by Usage_Month;