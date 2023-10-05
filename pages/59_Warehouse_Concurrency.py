import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.io as pio

from common import whitebear as wb

# Pate Setup 
wb.page_setup('Warehouse Concurrency Matrix last 30 Days')

# ----------------------------------------------------------------------
# Side Bar Menu Options 
# ----------------------------------------------------------------------
with st.sidebar: 
    account_selected     = wb.account_selector()
    #start_date, end_date = wb.date_selector()
# ----------------------------------------------------------------------
# Execute Query and Display Results 
# ----------------------------------------------------------------------
if account_selected:

    #df = wb.query('SN-warehouse-usage-versus-7-day-avg.sql',
    df = wb.query('SN-WH-concurrency.sql',
                  {
                    
                  }
                )

    fig2 = px.bar(df, x='WAREHOUSE_SIZE_SCORE', y='AVG_HIGHWATER_MARK_OF_CONCURRENCY_PER_MINUTE' , color='WAREHOUSE_NAME', title='Warehouse Concurrency Matrix')
    #fig2 = px.bar(df, x='HIGHWATER_MARK_OF_CONCURRENCY', y='AVG_HIGHWATER_MARK_OF_CONCURRENCY_PER_MINUTE' , color='WAREHOUSE_NAME', title='Warehouse Concurrency Matrix')
    
    #fig2 = px.bar(df, x='WAREHOUSE_SIZE_SCORE', y='HIGHWATER_MARK_OF_CONCURRENCY' , color='WAREHOUSE_NAME', title='Warehouse Concurrency Matrix')
    
    fig2.update_layout(barmode='stack', xaxis={'categoryorder':'total descending'})
    fig2.update_layout(barmode='relative')
    st.plotly_chart(fig2, theme="streamlit", use_container_width=True)

    fig3 = px.bar(df,  x='WAREHOUSE_NAME', y='AVG_HIGHWATER_MARK_OF_CONCURRENCY_PER_MINUTE', color='HIGHWATER_MARK_OF_CONCURRENCY', title='Warehouse by Name Concurrency Matrix')
    #fig2 = px.bar(df, x='HIGHWATER_MARK_OF_CONCURRENCY', y='AVG_HIGHWATER_MARK_OF_CONCURRENCY_PER_MINUTE' , color='WAREHOUSE_NAME', title='Warehouse Concurrency Matrix')
    
    #fig2 = px.bar(df, x='WAREHOUSE_SIZE_SCORE', y='HIGHWATER_MARK_OF_CONCURRENCY' , color='WAREHOUSE_NAME', title='Warehouse Concurrency Matrix')
    
    fig3.update_layout(barmode='stack', xaxis={'categoryorder':'total descending'})
    fig3.update_layout(barmode='relative')
    st.plotly_chart(fig3, theme="streamlit", use_container_width=True)


    st.dataframe(df)
else:
    st.info('Select an **Account** from the select box in the left sidebar.')