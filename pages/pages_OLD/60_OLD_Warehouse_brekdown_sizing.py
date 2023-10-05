import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.io as pio

from common import whitebear as wb

# Pate Setup 
wb.page_setup('Warehouse Breakdowh by Query Size in 30 Days')

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
    df = wb.query('SN-WH-breakdown-sizing.sql',
                  {
                    'start_date': start_date,
                    'end_date'  : end_date,
                  }
                )
    
    #chart_data = pd.DataFrame(np.random.randn(20, 3),columns=["a", "b", "c"])
    #chart_data = df #pd.DataFrame(np.random.randn(20, 3),columns=["a", "b", "c"])

    #st.bar_chart(chart_data)

    fig2 = px.bar(df, x='WAREHOUSE_NAME', y=['Percent 10GB Plus', 'Percent 1 to 10GB', 'Percent less than 0.5GB', 'Percent from 0.5GB to 1GB'] , color='WAREHOUSE_SIZE', title='Warehouse Breakdown')
    fig2.update_layout(barmode='stack', xaxis={'categoryorder':'total descending'})
    fig2.update_layout(barmode='relative')
    st.plotly_chart(fig2, theme="streamlit", use_container_width=True)

    fig3 = px.bar(df, x='WAREHOUSE_NAME', y='Percent from 0.5GB to 1GB' , color='WAREHOUSE_SIZE', title='Warehouse Breakdown - 0.5GB to 1GB')
    fig3.update_layout(barmode='stack', xaxis={'categoryorder':'total descending'})
    fig3.update_layout(barmode='relative')
    st.plotly_chart(fig3, theme="streamlit", use_container_width=True)


    st.dataframe(df)
else:
    st.info('Select an **Account** from the select box in the left sidebar.')