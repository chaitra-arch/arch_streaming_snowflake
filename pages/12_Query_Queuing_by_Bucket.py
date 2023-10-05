import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.io as pio
#pio.templates

from common import whitebear as wb

# Pate Setup 
wb.page_setup('Query Queueing Buckets Breakdown')

# ----------------------------------------------------------------------
# Side Bar Menu Options 
# ----------------------------------------------------------------------
with st.sidebar: 
    account_selected     = wb.account_selector()
    start_date, end_date = wb.date_selector()

# ----------------------------------------------------------------------
# Execute Query and Display Results 
# ----------------------------------------------------------------------
if account_selected:

    df = wb.query('SN-query-queue-by-bucket.sql',
                  {
                     'start_date': start_date,
                     'end_date'  : end_date,
                  }
                )

    fig2 = px.bar(df, x='CST_DATE', y='QUEUE_BUCKET', color='QUEUE_BUCKET', title='Query Queueing for Account by 30, 10, 5 seconds')
    #st.plotly_chart(fig2, template="plotly_white",use_container_width=True)

    

    fig2 = px.bar(df, x='CST_DATE', y='QUEUE_BUCKET', color='WAREHOUSE_NAME', title='Query Queuing by Warehouse')
    #st.plotly_chart(fig2, template="plotly_white",use_container_width=True)

    # execution_time_sec >= 1.0
    #st.write("Percentage of Large Scanning per Time Buckets")
    fig3 = px.bar(df, x='WAREHOUSE_NAME', y='QUERIES_RUN', color='QUEUE_BUCKET', title='Query Queuing by Warehouse (execution_time_sec >= 1.0)')
    st.plotly_chart(fig3, template="plotly_white",use_container_width=True)

    fig3 = px.bar(df, x='WAREHOUSE_NAME', y='PCT_OF_TOTAL', color='QUEUE_BUCKET', title='Query Queuing by Warehouse (PCT_OF_TOTAL)')
    st.plotly_chart(fig3, template="plotly_white",use_container_width=True)

    #fig2 = px.bar(df, x='WAREHOUSE_NAME', y=['QUEUED_MORE_THAN_30S','QUEUED_MORE_THAN_10S','QUEUED_MORE_THAN_5S'], title='Queueing by Warehouse for the Time Period Total')
    #st.plotly_chart(fig2, template="plotly_white",use_container_width=True)

    #fig2 = px.bar(df, x='WAREHOUSE_NAME', y='QUEUE_BUCKET', color='QUERY_TEXT', title='Queueing by Warehouse Color Query Text')
    #st.plotly_chart(fig2, template="plotly_white",use_container_width=True)

    #fig2 = px.bar(df, x='QUERY_TEXT', y='QUEUE_BUCKET', color='WAREHOUSE_NAME', title='Average All Query Exec Time for Loading by Warehouse')
    #st.plotly_chart(fig2, template="plotly_white",use_container_width=True)


    st.dataframe(df)
else:
    st.info('Select an **Account** from the select box in the left sidebar.')