import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.io as pio

from common import whitebear as wb

# Pate Setup 
wb.page_setup('Warehouse Scoring last 30 days TOP 30')

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

    #df = wb.query('SN-warehouse-usage-versus-7-day-avg.sql',
    df = wb.query('SN-WH-scoring.sql',
                  {
                    'start_date': start_date,
                    'end_date'  : end_date,
                  }
                )

    fig2 = px.bar(df, x='WAREHOUSE_NAME', y='WAREHOUSE_SIZE_SCORE' , color='WAREHOUSE_SIZE', title='Warehouse Scoring')
    fig2.update_layout(barmode='stack', xaxis={'categoryorder':'total descending'})
    fig2.update_layout(barmode='relative')
    st.plotly_chart(fig2, theme="streamlit", use_container_width=True)


    st.dataframe(df)
else:
    st.info('Select an **Account** from the select box in the left sidebar.')