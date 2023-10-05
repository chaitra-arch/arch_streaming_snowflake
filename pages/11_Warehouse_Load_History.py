import streamlit as st
import pandas as pd
import plotly.express as px

from common import whitebear as wb

# Pate Setup 
wb.page_setup('Warehouse Load History TOP 5 Queued')

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

    df = wb.query('SN-warehouse-load-history.sql',
                  {
                     'start_date': start_date,
                     'end_date'  : end_date,
                  }
                )

    fig2 = px.bar(df, x='START_TIME', y='AVG_RUNNING', color='WAREHOUSE_NAME', title='Warehouse Load History for TOP 10 Queued Load')
    st.plotly_chart(fig2, theme="streamlit", use_container_width=True)

    fig2 = px.bar(df, x='START_TIME', y='AVG_QUEUED_LOAD', color='WAREHOUSE_NAME', title='(blank is good) Warehouse Load History for TOP 10 Queued Waiting')
    fig2.update_layout(barmode='stack', xaxis={'categoryorder':'total ascending'})
    st.plotly_chart(fig2, theme="streamlit", use_container_width=True)

    fig2 = px.bar(df, x='START_TIME', y='AVG_BLOCKED', color='WAREHOUSE_NAME', title='(blank is good) Warehouse Load History for TOP 10 Queued Blocked')
    fig2.update_layout(barmode='stack', xaxis={'categoryorder':'total ascending'})
    st.plotly_chart(fig2, theme="streamlit", use_container_width=True)

    st.dataframe(df)
else:
    st.info('Select an **Account** from the select box in the left sidebar.')