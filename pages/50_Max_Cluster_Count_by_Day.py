import streamlit as st
import pandas as pd
import plotly.express as px

from common import whitebear as wb

# Pate Setup 
wb.page_setup('Max(Cluster Count) by Day > 1')

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

    df = wb.query('SN-max-cluster-count-by-day.sql',
                  {
                     'start_date': start_date,
                     'end_date'  : end_date,
                  }
                )

    fig2 = px.bar(df, x='QUERY_DAY', y='MAX_CLUSTER_COUNT', color='WAREHOUSE_SIZE', title='Warehouse size by Max Cluster Count > 1')
    st.plotly_chart(fig2, theme="streamlit", use_container_width=True)

    fig2 = px.bar(df, x='QUERY_DAY', y='MAX_CLUSTER_COUNT', color='WAREHOUSE_NAME', title='Clusters by Max Cluster Count > 1')
    st.plotly_chart(fig2, theme="streamlit", use_container_width=True)

    fig2 = px.bar(df, x='QUERY_DAY', y='AVG_CLUSTER_COUNT', color='WAREHOUSE_NAME', title='Clusters by Avg() Cluster Count')
    st.plotly_chart(fig2, theme="streamlit", use_container_width=True)

    fig2 = px.bar(df, x='QUERY_DAY', y='HOURLY_QUERY_COUNT', color='WAREHOUSE_NAME', title='Clusters by Hourly Query Count')
    st.plotly_chart(fig2, theme="streamlit", use_container_width=True)

    st.dataframe(df)
else:
    st.info('Select an **Account** from the select box in the left sidebar.')