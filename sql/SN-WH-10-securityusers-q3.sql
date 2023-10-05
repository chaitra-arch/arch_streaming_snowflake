
select user_name,
   first_authentication_factor || ' ' ||nvl(second_authentication_factor, '') as authentication_method
   , count(*) as CNT
    from snowflake.account_usage.login_history
    where is_success = 'YES'
    and user_name != 'WORKSHEETS_APP_USER'
    and user_name NOT LIKE '%GLOBAL%'
    and user_name NOT LIKE '%BATCH%'
    group by 1, authentication_method
    order by count(*) desc;
