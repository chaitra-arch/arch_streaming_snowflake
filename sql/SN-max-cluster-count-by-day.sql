--  Name:         SN-max-cluster-count-by-day.sql  
--  Created Date: 24-April-2023
--  Description:  Took one of the dashboard queries and put it in here to see if it would work in CAS2
------------------------------------------------------------------------------------------
// Max Cluster Count by Day - Percent of Queries
SELECT 
date_trunc('day', convert_timezone('America/Chicago', start_time::timestamp_ntz)) as query_day,
warehouse_size,
warehouse_name,
MAX(cluster_number) AS max_cluster_count, 
AVG(cluster_number) AS avg_cluster_count, 
MEDIAN(cluster_number) AS median_cluster_count, 
count(*) AS hourly_query_count 
FROM snowflake.account_usage.query_history q 
WHERE date(start_time) BETWEEN '{start_date}' AND '{end_date}'
GROUP BY 1,2,3
ORDER BY 1,4,2 DESC