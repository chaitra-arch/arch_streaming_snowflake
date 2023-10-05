import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.io as pio
import altair as alt

from common import whitebear as wb

# Pate Setup 
wb.page_setup('Warehouse Concurrency Details on WH ')

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
    

if account_selected:

    #df = wb.query('SN-warehouse-usage-versus-7-day-avg.sql',
    df = wb.query('SN-WH-87-concurrency-details-q0.sql',
                  {
                    'start_date': start_date,
                    'end_date'  : end_date,
                    'WAREHOUSE_NAME_IN' : wh_selected
                    
                  }
                )
    
    df1 = wb.query('SN-WH-87-concurrency-details.sql',
                  {
                     'start_date': start_date,
                     'end_date'  : end_date,
                     'WAREHOUSE_NAME_IN' : wh_selected 
                  }
                )#get_df()
    df2 = wb.query('SN-WH-87-concurrency-details-q2.sql',
                  {
                     'start_date': start_date,
                     'end_date'  : end_date,
                     'WAREHOUSE_NAME_IN' : wh_selected 
                  }
                )#get_df()
    
    

    fig1 = px.bar(df1, x='SLICE_FOR_1MIN', y='CNT' , color='CLUSTER_NUMBER', title='Cluster Concurrency Matrix by 1 min')
   
    fig1.update_layout(barmode='stack') #, xaxis={'categoryorder':'total descending'})
    #fig2.update_layout(barmode='relative')
    st.plotly_chart(fig1, theme="streamlit", use_container_width=True)
    st.divider()

    fig2 = px.bar(df2, x='SLICE_FOR_1MIN', y='CNT' , color='WAREHOUSE_NAME', title='Cluster Concurrency Matrix2')
   
    fig2.update_layout(barmode='stack') #, xaxis={'categoryorder':'total descending'})
    #fig2.update_layout(barmode='relative')
    st.plotly_chart(fig2, theme="streamlit", use_container_width=True)

    df2
    st.divider()

    fig2 = px.bar(df2, x='SLICE_FOR_1MIN', y='CNT' , color='WAREHOUSE_NAME', title='Cluster Concurrency Matrix2')
   
    fig2.update_layout(barmode='stack') #, xaxis={'categoryorder':'total descending'})
    #fig2.update_layout(barmode='relative')
    st.plotly_chart(fig2, theme="streamlit", use_container_width=True)

    df2
    st.divider()


     ##################
    #st.write("Percentage of Large Scanning per Time Buckets")
    chart = (
    alt.Chart(df1, title="Queries in Cluster")
    .mark_circle()
    .encode(
        alt.X("SLICE_FOR_1MIN:T", sort="descending"),
        alt.Y("CNT"),
        alt.Color("CLUSTER_NUMBER"),
        #alt.Size("CNT"), #, sort="descending"
        alt.Tooltip(["WAREHOUSE_NAME", "SLICE_FOR_1MIN", "CNT", "CLUSTER_NUMBER"]),
        #alt.Tooltip( "AVG_EXECUTION_TIME:O", format=",.2f"),
        #tooltip=[ 'AVG_EXECUTION_TIME', alt.Tooltip('AVG_EXECUTION_TIME:Q', format='.1%')]
        #alt.Text('AVG_EXECUTION_TIME', format='%'), #,.1f
        
    )
    .interactive()
    ).properties(description='chart', width=1500, height=800)
    st.altair_chart(chart, theme=None)
    #df1
    st.divider()
    st.divider()

    #fig2 = px.bar(df, x='INTERVAL_START', y=['SUM_QUEUED_TIME', 'SUM_EXECUTION_TIME'] , color='P95_TOTAL_DURATION' QUEUED_RATIO, title='Warehouse Concurrency Matrix1')
    fig2 = px.bar(df, x='INTERVAL_START', y='SUM_QUEUED_TIME' , color='QUEUED_RATIO', title='Warehouse Concurrency with QUEUED_RATIO (sum_queued_time/sum_total_duration)')
   
    fig2.update_layout(barmode='stack', xaxis={'categoryorder':'total descending'})
    #fig2.update_layout(barmode='relative')
    #st.plotly_chart(fig2, theme="streamlit", use_container_width=True)

    st.plotly_chart(fig2, template="plotly_white",use_container_width=True)

    fig3 = px.bar(df, x='INTERVAL_START', y='NUMJOBS' , color='CLUSTER_NUMBER',  title='Warehouse Concurrency Matrix2')

    #fig3.update_layout(barmode='stack', xaxis={'categoryorder':'total ascending'})
    #fig3.update_layout(barmode='relative')
    #fig3.update_layout(barmode='relative')
    #st.plotly_chart(fig3, template="plotly_white",use_container_width=True)
    st.divider()

    fig4 = px.bar(df, x='INTERVAL_START', y=['SUM_QUEUED_TIME', 'SUM_EXECUTION_TIME'] , color='QUERY_TYPE',  title='Warehouse Concurrency Matrix3')

    fig4.update_layout(barmode='stack', xaxis={'categoryorder':'total ascending'})
    #fig3.update_layout(barmode='relative')
    #fig3.update_layout(barmode='relative')
    #st.plotly_chart(fig4, template="plotly_white",use_container_width=True)
    st.divider()

    

    st.dataframe(df1)
    st.dataframe(df)

else:
    st.info('Select an **Account** from the select box in the left sidebar.')