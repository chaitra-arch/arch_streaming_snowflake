--  Name:         SN-max-hourly-query-count-by-cluster.sql  
--  Created Date: 24-April-2023
--  Description:  Took one of the dashboard queries and put it in here to see if it would work in CAS2
------------------------------------------------------------------------------------------
// Max Cluster Count by Day - Percent of Queries
SELECT 
date_trunc('day', convert_timezone('America/Chicago', start_time::timestamp_ntz)) as query_day,
warehouse_name,
count(*) AS hourly_query_count
FROM snowflake.account_usage.query_history q 
WHERE date(start_time) BETWEEN '{start_date}' AND '{end_date}'
GROUP BY 1,2
HAVING MAX(cluster_number) > 1
ORDER BY 3 DESC