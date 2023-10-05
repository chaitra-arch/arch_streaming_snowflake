--  Name:         SH-user-queries-sum.sql  
--  Created Date: 18-Feb-2023
--  Description:  <description>
------------------------------------------------------------------------------------------
 select TO_CHAR(DATE(j.created_on)) AS created_on
        ,s.statement_type
        ,COUNT(DISTINCT j.session_id) AS sessions
        ,COUNT(*) AS statement_count
        ,SUM(TO_NUMBER(j.total_duration / 600000, 10, 3)) AS tot_min
        FROM
            snowhouse_import.{deployment}.JOB_ETL_V J
            JOIN snowhouse_import.{deployment}.account_etl_v  a ON j.account_id = a.id
            JOIN snowhouse.product.statement_type s ON j.statement_properties = s.id
        WHERE
            a.name = '{account}'
            AND j.user_name = '{user_name}'
            AND j.created_on  BETWEEN '{start_date}' AND '{end_date}'  
        GROUP BY 1, 2
        ORDER BY 1