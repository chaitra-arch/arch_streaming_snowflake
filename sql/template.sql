------------------------------------------------------------------------------------------
--  Name:         template.sql  
--  Created Date: 26-Jan-2023
--  Description:  Example query using passed variables.  Variable names passed from python
--                are encased in curly brackes, e.g. {start_date}.   Passed variable can be 
--                anywhere in the query (schema name, table name, predicate value, column name, etc)
------------------------------------------------------------------------------------------
SELECT 
    current_timestamp() AS current_time
	,CURRENT_ACCOUNT() as account
	,CURRENT_REGION() as Region
	,CURRENT_USER() as User
	,CURRENT_ROLE() as role
	,CURRENT_WAREHOUSE() as warehouse
    ,'{start_date}' as start_date
    ,'{end_date}' as end_date
    ,'{timezone}' as timezone
