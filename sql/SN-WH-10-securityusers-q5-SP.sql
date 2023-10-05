--call util_db.public.SP_get_user_netpolicy();
/*
 -- Install the SP in to DB and Schema that will be called from Streamlit as
    call {db_name}.{sc_name}.SP_get_user_netpolicy();
*/
CREATE OR REPLACE PROCEDURE SP_get_user_netpolicy()
RETURNS TABLE()// VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER -- OWNER--
AS
--$$
DECLARE
l_var VARCHAR(9000);
l_name VARCHAR(9000);
//row_count INTEGER DEFAULT 0;
select_statement VARCHAR;
res2 RESULTSET;
res RESULTSET DEFAULT (SELECT name FROM snowflake.account_usage.users where deleted_on is null and name != 'SNOWFLAKE');
res3 RESULTSET DEFAULT (CREATE OR REPLACE TABLE USERS_WITH_NET_POLICY (USER_NAME string, POLICY string, level string, CR_DT DATE DEFAULT CURRENT_DATE()) );
c1 CURSOR FOR res;
select_statement2 string;
//res3 RESULTSET;
getPolicy string;
BEGIN
select_statement2 := '';
  FOR names IN c1 DO
    l_name := names.name;
    //select_statement2 := 'SELECT CONTAINS(''' || l_name || ''', ''@'');';
    //res3 := (EXECUTE IMMEDIATE :select_statement);
    IF (EXISTS (SELECT CONTAINS(:l_name, '@'))) THEN
      //RETURN 'Executing some code';
      select_statement := 'SHOW PARAMETERS like ''NETWORK_POLICY'' for user "' || l_name || '";';
  ELSE
      //RETURN 'Resource monitor already present hence insert skipped';
      select_statement := 'SHOW PARAMETERS like ''NETWORK_POLICY'' for user ' || l_name || ';';
  END IF;
  res2 := (EXECUTE IMMEDIATE :select_statement);
  getPolicy := 'INSERT INTO USERS_WITH_NET_POLICY (USER_NAME, POLICY, LEVEL) SELECT ''' || l_name || ''' as name, case when unicode("value") = ''0'' then ''no_policy'' else "value" end as POLICY, case when unicode("value") = ''0'' then ''no_policy_level'' else "level" end as level  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));';
  //res3 := (EXECUTE IMMEDIATE :getPolicy);
  res2 := (EXECUTE IMMEDIATE :getPolicy);
  select_statement2 := select_statement2 || ' ;' || select_statement;
  END FOR;
  RETURN TABLE(res2);//select_statement;
END;
--
