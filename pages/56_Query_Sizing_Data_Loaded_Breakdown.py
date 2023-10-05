import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.io as pio
#pio.templates

from common import whitebear as wb

# Pate Setup 
wb.page_setup('Warehouse Querys Data Loading Size for Warehouse Breakdown')

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

    df = wb.query('SN-query-sizing-breakdown.sql',
                  {
                     'start_date': start_date,
                     'end_date'  : end_date,
                  }
                )

    fig2 = px.bar(df, x=['PERCENT_10GB_PLUS', 'PERCENT_1_TO_10GB', 'PERCENT_LESS_500MB', 'PERCENT_500MB_TO_1GB'], y='WAREHOUSE_SIZE', title='Warehouse Query Sizing Breakdown For Loading')
    st.plotly_chart(fig2, template="plotly_white",use_container_width=True)

    fig5 = px.bar(df, x= 'WAREHOUSE_NAME', y= ['PERCENT_10GB_PLUS', 'PERCENT_1_TO_10GB', 'PERCENT_LESS_500MB', 'PERCENT_500MB_TO_1GB'], title='Warehouse Query Sizing Breakdown For Loading')
    fig5.update_layout(barmode='stack', xaxis={'categoryorder':'total descending'})
    st.plotly_chart(fig5, template="plotly_white",use_container_width=True)

    txt1 = st.text_area('Verify', height = 200, value = '''
    1. Check if X-Small WH utilizing 'PERCENTILE_10GB_PLUS'
    2. Check if larger WHs are utilizing smaler PERCENTLIES
    3. Check if WHs are having mixed utilizations:
        X-SMALL or SMALL are having  'PERCENTILE_10GB_PLUS'
        2,3,4 X-LARGE are having 'PERCENTILE_1_TO_10GB' or less
    ''')

    fig3 = px.bar(df, x='WAREHOUSE_NAME', y='AVG_QUERY_COST' , color='WAREHOUSE_SIZE', title='AVG Query Cost')
    fig3.update_layout(barmode='stack', xaxis={'categoryorder':'total descending'})
    fig3.update_layout(barmode='relative')
    st.plotly_chart(fig3, theme="streamlit", use_container_width=True)

    fig2 = px.bar(df, x='PERCENT_10GB_PLUS', y='WAREHOUSE_SIZE', color='WAREHOUSE_NAME', title='Large Data Loading Queries > 10GB by Warehouse')
    st.plotly_chart(fig2, template="plotly_white",use_container_width=True)

    fig2 = px.bar(df, x='AVG_ALL_EXE_TIME', y='WAREHOUSE_SIZE', color='WAREHOUSE_NAME', title='Average All Query Exec Time for Loading by Warehouse')
    st.plotly_chart(fig2, template="plotly_white",use_container_width=True)

    st.dataframe(df)
else:
    st.info('Select an **Account** from the select box in the left sidebar.')