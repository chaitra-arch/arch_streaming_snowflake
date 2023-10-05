select
    user_name,
    error_message,
    count(*) num_of_failures
from
    snowflake.account_usage.login_history
where
    is_success = 'NO'
group by
    user_name,
    error_message
order by
    num_of_failures desc;