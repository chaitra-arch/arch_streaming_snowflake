import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.io as pio

from common import whitebear as wb

# Pate Setup 
wb.page_setup('Warehouse Usage Last Week Versus Previous 7 Day AVG()')

# ----------------------------------------------------------------------
# Side Bar Menu Options 
# ----------------------------------------------------------------------
with st.sidebar: 
    account_selected     = wb.account_selector()

# ----------------------------------------------------------------------
# Execute Query and Display Results 
# ----------------------------------------------------------------------
if account_selected:

    df = wb.query('SN-warehouse-usage-versus-7-day-avg.sql',
                  {
                  }
                )

    fig2 = px.bar(df, x='WAREHOUSE_NAME', y='VARIANCE_TO_7_DAY_AVERAGE' , title='Warehouse Usage Trending Ranges VS 7 Day AVG()')
    fig2.update_layout(barmode='stack', xaxis={'categoryorder':'total descending'})
    fig2.update_layout(barmode='relative')
    st.plotly_chart(fig2, theme="streamlit", use_container_width=True)

    fig2 = px.bar(df, x='DATE', y='VARIANCE_TO_7_DAY_AVERAGE', color='WAREHOUSE_NAME', title='Warehouse Usage Trending On Each Day VS 7 Day AVG()')
    st.plotly_chart(fig2, theme="streamlit", use_container_width=True)

    fig2 = px.bar(df, x='DATE', y='CREDITS_USED', color='WAREHOUSE_NAME', title='Warehouse Credits Used by Day')
    st.plotly_chart(fig2, theme="streamlit", use_container_width=True)

    st.dataframe(df)
else:
    st.info('Select an **Account** from the select box in the left sidebar.')