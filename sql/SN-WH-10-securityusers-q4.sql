
    select --user_name,
   first_authentication_factor || ' ' ||nvl(second_authentication_factor, '') as authentication_method
   , count(*) as CNT
    from snowflake.account_usage.login_history
    where is_success = 'YES'
    and user_name != 'WORKSHEETS_APP_USER'
    group by  authentication_method
    order by count(*) desc;