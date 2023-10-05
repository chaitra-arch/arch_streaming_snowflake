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
    start_date, end_date = wb.date_selector()
# ----------------------------------------------------------------------
# Execute Query and Display Results 
# ----------------------------------------------------------------------

st.write('WH Full Analysis', 'This report may run a few minutes, the process time is depending on the selected data range and the current WH size.')

df_wh = wb.query('SN-WH-list-q0.sql',
                  {
                     'start_date': start_date,
                     'end_date'  : end_date,
                  }
                )#get_df0()

    #duckdb.query("SELECT * FROM nyc where trip_distance>10").df()

if "visibility" not in st.session_state:
    st.session_state.visibility = "visible"
    st.session_state.disabled = True

col1, col2 = st.columns(2)

with col1:
    st.checkbox("Disable excluding WHs", key="disabled")
    #st.radio(
    #    "Set selectbox label visibility 👉",
    #    key="visibility",
    #    options=["visible", "hidden", "collapsed"],
    #)
    print('test')

with col2:
    wh_selected = st.selectbox('Select to exclude WHs',df_wh['WAREHOUSE_NAME'] , label_visibility=st.session_state.visibility,
        disabled=st.session_state.disabled,) #) + ' MONTH_CREDITS(' + str(df0['CREDITS'][1])+ ')')
    st.write('You selected:', wh_selected)
    options = st.multiselect('Select to exclude WHs',df_wh['WAREHOUSE_NAME'], label_visibility=st.session_state.visibility,
        disabled=st.session_state.disabled,)
    

    option_s = "','".join(options)


    st.write('You selected:', option_s)

if account_selected:

    #df = wb.query('SN-warehouse-usage-versus-7-day-avg.sql',
    df = wb.query('SN-WH-87-concurrency.sql',
                  {
                    
                  }
                )
    
    df1 = wb.query('SN-WH-87-concurrency-q1.sql',
                  {
                     'start_date': start_date,
                     'end_date'  : end_date,
                     'WAREHOUSE_NAME_IN' : option_s # wh_selected
                  }
                )#get_df()

    fig1 = px.bar(df1, x='WAREHOUSE_NAME', y='QUERIES' , color='CLUSTER_NUMBER', title='Cluster Concurrency Matrix1')
   
    fig1.update_layout(barmode='stack', xaxis={'categoryorder':'total descending'})
    #fig2.update_layout(barmode='relative')
    st.plotly_chart(fig1, theme="streamlit", use_container_width=True)
    st.divider()

    fig2 = px.bar(df, x='INTERVAL_START', y=['SUM_QUEUED_TIME', 'SUM_EXECUTION_TIME'] , color='P95_TOTAL_DURATION', title='Warehouse Concurrency Matrix1')
   
    fig2.update_layout(barmode='stack', xaxis={'categoryorder':'total descending'})
    #fig2.update_layout(barmode='relative')
    st.plotly_chart(fig2, theme="streamlit", use_container_width=True)

    #st.plotly_chart(fig2, template="plotly_white",use_container_width=True)

    fig3 = px.bar(df, x='INTERVAL_START', y='NUMJOBS' , color='CLUSTER_NUMBER',  title='Warehouse Concurrency Matrix2')

    #fig3.update_layout(barmode='stack', xaxis={'categoryorder':'total ascending'})
    #fig3.update_layout(barmode='relative')
    #fig3.update_layout(barmode='relative')
    st.plotly_chart(fig3, template="plotly_white",use_container_width=True)
    st.divider()

    fig4 = px.bar(df, x='INTERVAL_START', y=['SUM_QUEUED_TIME', 'SUM_EXECUTION_TIME'] , color='QUERY_TYPE',  title='Warehouse Concurrency Matrix3')

    fig4.update_layout(barmode='stack', xaxis={'categoryorder':'total ascending'})
    #fig3.update_layout(barmode='relative')
    #fig3.update_layout(barmode='relative')
    st.plotly_chart(fig4, template="plotly_white",use_container_width=True)
    st.divider()

    

    st.dataframe(df1)
    st.dataframe(df)
else:
    st.info('Select an **Account** from the select box in the left sidebar.')