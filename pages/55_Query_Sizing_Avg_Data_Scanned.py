import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.io as pio
#pio.templates

from common import whitebear as wb

# Pate Setup 
wb.page_setup('Warehouse Query Sizes Based on Data Scanned Breakdown')

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

    df = wb.query('SN-query-sizing-avg-data-scan.sql',
                  {
                     'start_date': start_date,
                     'end_date'  : end_date,
                  }
                )

    fig2 = px.bar(df, x='END_DATE_CST', y='AVG_QUERY_SIZE', title='Query Size AVG() MB Scanned By Date Total for Account')
    st.plotly_chart(fig2, template="plotly_white",use_container_width=True)

    fig2 = px.bar(df, x='END_DATE_CST', y='MED_QUERY_SIZE', color='WAREHOUSE_NAME', title='Median Size MB Scanned per Query by Warehouse')
    st.plotly_chart(fig2, template="plotly_white",use_container_width=True)

    fig2 = px.bar(df, x='END_DATE_CST', y='AVG_QUERY_SIZE', color='WAREHOUSE_NAME', title='Average Size MB Scanned per Query by Warehouse')
    st.plotly_chart(fig2, template="plotly_white",use_container_width=True)

    fig2 = px.bar(df, x='WAREHOUSE_NAME', y='AVG_BYTES', color='WAREHOUSE_NAME', title='Total AVG MB Scanned in Queries by Warehouse for the Time Period')
    fig2.update_layout(barmode='stack', xaxis={'categoryorder':'total descending'})
    fig2.update_layout(barmode='relative')
    st.plotly_chart(fig2, template="plotly_white",use_container_width=True)

    fig2 = px.bar(df, x='END_DATE_CST', y='COUNT_WAIT', color='WAREHOUSE_NAME', title='Number of Queries in COUNT_WAIT by Warehouse')
    st.plotly_chart(fig2, template="plotly_white",use_container_width=True)

    fig2 = px.bar(df, x='END_DATE_CST', y='PCT_WAIT_COUNT', color='WAREHOUSE_NAME', title='Percentage of Queries in COUNT_WAIT by Warehouse')
    st.plotly_chart(fig2, template="plotly_white",use_container_width=True)


    st.dataframe(df)
else:
    st.info('Select an **Account** from the select box in the left sidebar.')