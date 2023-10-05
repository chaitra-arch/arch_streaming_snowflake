
select
    user_name,
    to_varchar(CLIENT_IP) as CLIENT_IP
    ,IS_SUCCESS
    ,error_message
    ,(count(*))::int cnt_ip
from
    snowflake.account_usage.login_history
where CLIENT_IP not like '10.%'
group by 1,2,3,4
;
