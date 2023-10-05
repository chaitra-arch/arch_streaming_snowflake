import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.io as pio

from common import whitebear as wb

# Pate Setup 
wb.page_setup('Warehouse Breakdowh by Query Size in 30 Days Category')

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
    df = wb.query('SN-WH-breakdown-sizing-unpivot.sql',
                  {
                    'start_date': start_date,
                    'end_date'  : end_date,
                  }
                )
    
    #chart_data = pd.DataFrame(np.random.randn(20, 3),columns=["a", "b", "c"])
    #chart_data = df #pd.DataFrame(np.random.randn(20, 3),columns=["a", "b", "c"])

    #st.bar_chart(chart_data)

    #fig2 = px.bar(df, x='WAREHOUSE_NAME', y='Percent 10GB Plus' , color='WAREHOUSE_SIZE', title='Warehouse Breakdown - 10GB Plus')
    fig2 = px.bar(df, x='WH_AND_SIZE', y='WAREHOUSE_SIZING' , color='SIZING', title='Warehouse Breakdown - by Sizing Percentile')
    
    #fig2.update_layout(barmode='stack', xaxis={'categoryorder':'total descending'})
    fig2.update_layout(barmode='stack')
    fig2.update_layout(barmode='relative')
    st.plotly_chart(fig2, theme="streamlit", use_container_width=True)

    txt1 = st.text_area('Verify', height = 200, value = '''
    1. Check if X-Small WH utilizing 'PERCENTILE_10GB_PLUS'
    2. Check if larger WHs are utilizing smaler PERCENTLIES
    3. Check if WHs are having mixed utilizations:
        X-SMALL or SMALL are having  'PERCENTILE_10GB_PLUS'
        2,3,4 X-LARGE are having 'PERCENTILE_1_TO_10GB' or less
    ''')
    #st.write('Sentiment:', (txt))

    fig3 = px.bar(df, x='WH_AND_SIZE', y='AVG_QUERY_COST' , color='WAREHOUSE_SIZE', title='AVG Query Cost')
    fig3.update_layout(barmode='stack', xaxis={'categoryorder':'total descending'})
    fig3.update_layout(barmode='relative')
    st.plotly_chart(fig3, theme="streamlit", use_container_width=True)


    st.dataframe(df)
else:
    st.info('Select an **Account** from the select box in the left sidebar.')