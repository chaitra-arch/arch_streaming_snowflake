import streamlit as st
import pandas as pd
import plotly.express as px

from common import whitebear as wb

# Pate Setup 
wb.page_setup('Top SQL Execution Metrics Last Seven Days')

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
left(query_text, 40) q_short, 
warehouse_name,
query_type, 
database_name, 
user_name, 
COUNT(*) AS query_count,
--ROUND(AVG(total_elapsed_time)/(1000*60),0) avg_elapsed_min, 
ROUND(AVG(execution_time)/(1000*60),0) AS avg_exec_min, 
ROUND(SUM(execution_time)/(1000*60*60),0) AS total_exec_hours,
ROUND(AVG(bytes_spilled_to_local_storage) / power(1024,3),4) AS avg_gb_spilled_to_local_storage,
ROUND(AVG(bytes_spilled_to_remote_storage) / power(1024,3),4) AS avg_gb_spilled_to_remote_storage,
ROUND(AVG((partitions_scanned / NULLIF(partitions_total,0))),2) * 100 AS avg_partition_scan_ratio,
ROUND(AVG(partitions_scanned),0) AS avg_partitions_scanned, 
ROUND(AVG(partitions_total),0) AS avg_partitions_total,
ROUND(AVG(BYTES_SCANNED) / POWER(2, 30), 0) AS avg_GB_scanned,
TO_CHAR(MIN(start_time),'YYYY-MM-DD hh24:mi') AS earliest_start_time, 
TO_CHAR(MAX(start_time),'YYYY-MM-DD hh24:mi') AS latest_start_time, 
MAX(query_id) latest_query_id
FROM   snowflake.account_usage.query_history 
WHERE  start_time > dateadd(day, -7, current_date())
AND query_type <> 'COMMIT'
GROUP BY 1,2,3,4,5,6
ORDER BY total_exec_hours DESC
LIMIT 20
""",)


# Show Charts
    #fig2 = px.bar(df, x='TOTAL_EXEC_HOURS', y='QUERY_TEXT', color='WAREHOUSE_NAME', title='Top SQL Execution Times by Warehouse')
    fig2 = px.bar(df, x='TOTAL_EXEC_HOURS', y='Q_SHORT', color='WAREHOUSE_NAME', title='Top SQL Execution Times by Warehouse')
    
    fig2.update_layout(barmode='stack', yaxis={'categoryorder':'total descending'})
    fig2.update_layout(barmode='relative')
    st.plotly_chart(fig2, theme="streamlit", use_container_width=True)

    #fig2 = px.bar(df, x='AVG_EXEC_MIN', y='QUERY_TEXT', color='WAREHOUSE_NAME', title='Top SQL Execution In Minutes')
    fig2 = px.bar(df, x='AVG_EXEC_MIN', y='Q_SHORT', color='WAREHOUSE_NAME', title='Top SQL Execution In Minutes')
    
    fig2.update_layout(barmode='stack', yaxis={'categoryorder':'total descending'})
    fig2.update_layout(barmode='relative')
    st.plotly_chart(fig2, theme="streamlit", use_container_width=True)

    fig2 = px.bar(df, x='AVG_GB_SPILLED_TO_LOCAL_STORAGE', y='Q_SHORT', color='WAREHOUSE_NAME', title='Top SQL GB Spilled to Local')
    fig2.update_layout(barmode='stack', yaxis={'categoryorder':'total descending'})
    fig2.update_layout(barmode='relative')
    st.plotly_chart(fig2, theme="streamlit", use_container_width=True)

    fig2 = px.bar(df, x='AVG_GB_SPILLED_TO_REMOTE_STORAGE', y='Q_SHORT', color='WAREHOUSE_NAME', title='Top SQL GB Spilled to Remote')
    fig2.update_layout(barmode='stack', yaxis={'categoryorder':'total descending'})
    fig2.update_layout(barmode='relative')
    st.plotly_chart(fig2, theme="streamlit", use_container_width=True)

    fig2 = px.bar(df, x='AVG_PARTITION_SCAN_RATIO', y='Q_SHORT', color='WAREHOUSE_NAME', title='Top SQL Partitions Scanned Ratio')
    fig2.update_layout(barmode='stack', yaxis={'categoryorder':'total descending'})
    fig2.update_layout(barmode='relative')
    st.plotly_chart(fig2, theme="streamlit", use_container_width=True)

    fig2 = px.bar(df, x='AVG_PARTITIONS_SCANNED', y='Q_SHORT', color='WAREHOUSE_NAME', title='Top SQL Partitions Scanned')
    fig2.update_layout(barmode='stack', yaxis={'categoryorder':'total descending'})
    fig2.update_layout(barmode='relative')
    st.plotly_chart(fig2, theme="streamlit", use_container_width=True)

    fig2 = px.bar(df, x='AVG_GB_SCANNED', y='Q_SHORT', color='WAREHOUSE_NAME', title='Top SQL Average GB Scanned')
    fig2.update_layout(barmode='stack', yaxis={'categoryorder':'total descending'})
    fig2.update_layout(barmode='relative')
    st.plotly_chart(fig2, theme="streamlit", use_container_width=True)

    fig2 = px.bar(df, x='TOTAL_EXEC_HOURS', y='Q_SHORT', color='WAREHOUSE_NAME', title='Top SQL Total Execution Hours')
    fig2.update_layout(barmode='stack', yaxis={'categoryorder':'total descending'})
    fig2.update_layout(barmode='relative')
    st.plotly_chart(fig2, theme="streamlit", use_container_width=True)

# Show dataframe
    st.dataframe(df)
else:
    st.info('Select an **Account** from the select box in the left sidebar.')
