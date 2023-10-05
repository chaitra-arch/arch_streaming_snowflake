select distinct(WAREHOUSE_NAME)
from SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY 
where --END_TIME >=  '{end_date}'  --dateadd(DAY, -30, current_date())
end_time  BETWEEN'{start_date}' AND '{end_date}'
and warehouse_name not in ('CLOUD_SERVICES_ONLY')
ORDER BY WAREHOUSE_NAME
;