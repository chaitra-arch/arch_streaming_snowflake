import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.io as pio

from common import whitebear as wb

# Pate Setup 
wb.page_setup('Max Hourly Query Count for Clusters With Max Warehouse Count Above 1')

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

    df = wb.query('SN-max-hourly-query-count-by-cluster.sql',
                  {
                     'start_date': start_date,
                     'end_date'  : end_date,
                  }
                )

    fig2 = px.bar(df, x='HOURLY_QUERY_COUNT', y='WAREHOUSE_NAME' , title='Warehouse Clusters With High Sum Query Count Totals Over ALL Days')
    st.plotly_chart(fig2, theme="streamlit", use_container_width=True)

    fig2 = px.bar(df, x='QUERY_DAY', y='HOURLY_QUERY_COUNT', color='WAREHOUSE_NAME', title='Warehouse Clusters Total Query Count Per Day')
    st.plotly_chart(fig2, theme="streamlit", use_container_width=True)

    fig2 = px.bar(df, x='QUERY_DAY', y='HOURLY_QUERY_COUNT' , title='Total Query Count Across ALL Warehouse Clusters')
    st.plotly_chart(fig2, theme="streamlit", use_container_width=True)

    st.dataframe(df)
else:
    st.info('Select an **Account** from the select box in the left sidebar.')