select user_name, REPORTED_CLIENT_TYPE,
count(*) as cnt
from SNOWFLAKE.ACCOUNT_USAGE.login_history
where event_timestamp > date_trunc(month, current_date)
group by 1,2 order by 1 
;