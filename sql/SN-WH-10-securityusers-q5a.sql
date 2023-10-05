--call util_db.public.SP_get_user_netpolicy();
call DEMO_DB.PUBLIC.SP_get_user_netpolicy();
/*
 -- Install the SP in to DB and Schema that will be called from Streamlit as
    call {db_name}.{sc_name}.SP_get_user_netpolicy();
*/

/*
CREATE OR REPLACE PROCEDURE get_user_netpolicy()
  RETURNS variant NOT null
  LANGUAGE javascript
  EXECUTE as  CALLER
  AS     
  $$ 
    // This variable will hold a JSON data structure that holds ONE row.
    var rowAsJson = {};
    // This array will contain all the rows.
    var arrayOfRows = [];
    // This variable will hold a JSON data structure that we can return as
    // a VARIANT.
    // This will contain ALL the rows in a single "value".
    var tableAsJson = {};
  
    // Define initial command to get all user names
    var selectUsers = "SELECT name FROM snowflake.account_usage.users where deleted_on is null and name != 'SNOWFLAKE';";                                                  
    var selectUsersCommand = snowflake.createStatement( {sqlText: selectUsers} );
    
    // Execute the SQL command
    var userNames = selectUsersCommand.execute();

    //
     var tblWithData = 'CREATE OR REPLACE TABLE PUBLIC.USERS_WITH_NET_POLICY (USER_NAME string, CR_DT DATE DEFAULT CURRENT_DATE());';

    var getPolicyCommand2 = snowflake.createStatement( {sqlText: tblWithData} );

    var TBLnetPolicy = getPolicyCommand2.execute();
    //
    
    // Loop through the results, processing one row at a time... 
    while (userNames.next())  {
       var userName = userNames.getColumnValue(1);
       
       // Add result to json of rows
       rowAsJson = {};
       rowAsJson["name"] = userName;
       
       // Create SQL statement to show network policy
       var showPolicy = `SHOW PARAMETERS like 'NETWORK_POLICY' for user "` + userName + `";`;
       var showPolicyCommand = snowflake.createStatement( {sqlText: showPolicy} );
       
       showPolicyCommand.execute();
       
       // Use result scan to get the data and account for not truly null values. 
       // This SQL uses the UNICODE function to workaround a user with no network   
       // policy not returning a true null value
       var getPolicy = `SELECT case when unicode("value") = '0' then 'no_policy' else "value" end, case when unicode("value") = '0' then 'no_policy_level' else "level" end  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));`;
       var getPolicyCommand = snowflake.createStatement( {sqlText: getPolicy} );
       
       // Excute command to get network policy and the level
       var netPolicy = getPolicyCommand.execute();
       netPolicy.next();
       var netPolicyName = netPolicy.getColumnValue(1);
       var netPolicyLevel = netPolicy.getColumnValue(2);
       
       // Add network policy, leevel to JSON
       rowAsJson["network_policy"] = netPolicyName;
       rowAsJson["policy_level"] = netPolicyLevel;
       
       // Add the result to the array of rows
       arrayOfRows.push(rowAsJson)
       //
       var rowAsJson1 = "test";
        var insertInTbl = 'INSERT INTO USERS_WITH_NET_POLICY(USER_NAME) VALUES (' + rowAsJson1 + ');';

        //

        var getPolicyCommand3 = snowflake.createStatement( {sqlText: insertInTbl} );

        var INSTBLnetPolicy = getPolicyCommand3.execute();

       //
       }
    tableAsJson = { "users": arrayOfRows }

    //var tblWithData = 'CREATE TABLE PUBLIC.USERS_WITH_NET_POLICY (USER_NAME STRING, NETWORK_POLICY STRING, policy_level STRING, CR_DT DATE DEFAULT CURRENT_DATE());';
   

    
    
  return  tableAsJson;
  $$
;
--

*/