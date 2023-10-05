import streamlit as st
import pandas as pd
import plotly.express as px

from common import whitebear as wb

# Pate Setup 
wb.page_setup('Top 50 SQL by GB Scanned Last Seven Days')

# st.info('Select an **Account** from the select box in the left sidebar.')
# ----------------------------------------------------------------------
# Side Bar Menu Options 
# ----------------------------------------------------------------------
with st.sidebar:
    account_selected = wb.account_selector() 

# ----------------------------------------------------------------------
# Execute Query and Display Results 
# ----------------------------------------------------------------------
if account_selected:
    st.session_state.connection = account_selected['connection']
    df = wb.run_query("""SELECT  
query_text, 
warehouse_name,
query_type, 
database_name, 
user_name,
ROUND(AVG(BYTES_SCANNED) / POWER(2, 30), 0) AS avg_GB_scanned,
ROUND(SUM(BYTES_SCANNED) / POWER(2, 30), 0) AS total_GB_scanned,
ROUND(AVG(bytes_spilled_to_remote_storage) / power(1024,3),4) AS avg_gb_spilled_to_remote_storage,
ROUND(SUM(bytes_spilled_to_remote_storage) / power(1024,3),2) AS total_gb_spilled_to_remote_storage,
ROUND(AVG(bytes_spilled_to_local_storage) / power(1024,3),4) AS avg_gb_spilled_to_local_storage,
COUNT(*) AS query_count,
--ROUND(AVG(total_elapsed_time)/(1000*60),0) avg_elapsed_min, 
ROUND(AVG(execution_time)/(1000*60),0) AS avg_exec_min, 
ROUND(SUM(execution_time)/(1000*60),0) AS total_exec_minutes,
ROUND(AVG((partitions_scanned / NULLIF(partitions_total,0))),2) * 100 AS avg_partition_scan_ratio,
ROUND(AVG(partitions_scanned),0) AS avg_partitions_scanned, 
ROUND(AVG(partitions_total),0) AS avg_partitions_total,
TO_CHAR(MIN(start_time),'YYYY-MM-DD hh24:mi') AS earliest_start_time, 
TO_CHAR(MAX(start_time),'YYYY-MM-DD hh24:mi') AS latest_start_time, 
MAX(query_id) latest_query_id
FROM   snowflake.account_usage.query_history 
WHERE  start_time > dateadd(day, -7, current_date())
GROUP BY 1,2,3,4,5
ORDER BY total_GB_scanned DESC
LIMIT 50
""",)


# Show Chart
    fig2 = px.bar(df, x='QUERY_TEXT', y=['TOTAL_GB_SCANNED'] , title='Top SQL by GB Scanned')
    st.plotly_chart(fig2, theme="streamlit", use_container_width=True)

    fig2 = px.bar(df, x='USER_NAME', y=['TOTAL_GB_SCANNED'] , title='Top Users by GB Scanned')
    st.plotly_chart(fig2, theme="streamlit", use_container_width=True)

    fig2 = px.bar(df, x='DATABASE_NAME', y=['TOTAL_GB_SCANNED'] , color='WAREHOUSE_NAME', title='Top Databases by GB Scanned')
    st.plotly_chart(fig2, theme="streamlit", use_container_width=True)


# Show dataframe
    st.dataframe(df)
else:
    st.info('Select an **Account** from the select box in the left sidebar.')
