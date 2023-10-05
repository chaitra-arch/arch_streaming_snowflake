import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.io as pio
import altair as alt

from common import whitebear as wb

# Pate Setup 
wb.page_setup('Warehouse Concurrency Matrix')

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

    
    
    df1 = wb.query('SN-WH-87-concurrency-q1.sql',
                  {
                     'start_date': start_date,
                     'end_date'  : end_date,
                     'WAREHOUSE_NAME_IN' : option_s # wh_selected
                  }
                )#get_df()
    
    df2 = wb.query('SN-WH-87-concurrency-q2.sql',
                  {
                     'start_date': start_date,
                     'end_date'  : end_date,
                     'WAREHOUSE_NAME_IN' : option_s # wh_selected
                  }
                )#get_df()
    
    df3 = wb.query('SN-WH-87-concurrency-details-q2.sql',
                  {
                     'start_date': start_date,
                     'end_date'  : end_date,
                     'WAREHOUSE_NAME_IN' : option_s 
                  }
                )#get_df()
    
    fig2 = px.bar(df3, x='SLICE_FOR_1MIN', y='CNT' , color='WAREHOUSE_NAME', title='Cluster Concurrency Matrix2')
   
    fig2.update_layout(barmode='stack') #, xaxis={'categoryorder':'total descending'})
    #fig2.update_layout(barmode='relative')
    st.plotly_chart(fig2, theme="streamlit", use_container_width=True)

    #df2
    st.divider()

    st.divider()
    st.write("TOTAL_ELAPSED_TIME_AVG per Cluster")
    st.dataframe(df2)
    st.divider()

    fig1 = px.bar(df1, x='WAREHOUSE_NAME', y='QUERIES' , color='CLUSTER_NUMBER', title='Cluster Concurrency with Clusters')
   
    fig1.update_layout(barmode='stack') #, xaxis={'categoryorder':'total descending'})
    #fig2.update_layout(barmode='relative')
    st.plotly_chart(fig1, theme="streamlit", use_container_width=True)
    st.divider()

    fig2 = px.bar(df1, x='WAREHOUSE_NAME', y='AVG_EXECUTION_TIME' , color='CLUSTER_NUMBER', title='Cluster Concurrency with AVG Execution Time')
   
    fig2.update_layout(barmode='stack') #, xaxis={'categoryorder':'total descending'})
    #fig2.update_layout(barmode='relative')
    st.plotly_chart(fig2, theme="streamlit", use_container_width=True)
    st.divider()

    fig3 = px.bar(df1, x='WAREHOUSE_NAME', y='AVG_EXECUTION_TIME' , color='MEDIAN_QUEUED_TIME', title='Cluster Concurrency with Median Qued Time')
   
    fig3.update_layout(barmode='stack') #, xaxis={'categoryorder':'total descending'})
    #fig2.update_layout(barmode='relative')
    st.plotly_chart(fig3, theme="streamlit", use_container_width=True)
    st.divider()


     ##################
    #st.write("Percentage of Large Scanning per Time Buckets")
    

    tab1, tab2 = st.tabs(["With Cluster and Execution Time", "Queries Only"])

    with tab1:
        chart = (
    alt.Chart(df1, title="Queries in Cluster")
    .mark_circle()
    .encode(
        alt.X("WAREHOUSE_NAME:O", sort="descending"),
        alt.Y("QUERIES"),
        alt.Color("CLUSTER_NUMBER"),
        alt.Size("AVG_EXECUTION_TIME"), #, sort="descending"
        alt.Tooltip(["WAREHOUSE_NAME", "QUERIES", "AVG_EXECUTION_TIME", "CLUSTER_NUMBER"]),
        #alt.Tooltip( "AVG_EXECUTION_TIME:O", format=",.2f"),
        #tooltip=[ 'AVG_EXECUTION_TIME', alt.Tooltip('AVG_EXECUTION_TIME:Q', format='.1%')]
        #alt.Text('AVG_EXECUTION_TIME', format='%'), #,.1f
        
    )
    .interactive()
    ).properties(description='chart', width=1500, height=800)
        st.altair_chart(chart, theme="streamlit", use_container_width=True)
    with tab2:
        chart = (
    alt.Chart(df1, title="Queries in Cluster")
    .mark_circle()
    .encode(
        alt.X("WAREHOUSE_NAME:O", sort="descending"),
        alt.Y("QUERIES"),
        #alt.Color("CLUSTER_NUMBER"),
        #alt.Size("AVG_EXECUTION_TIME"), #, sort="descending"
        alt.Tooltip(["WAREHOUSE_NAME", "QUERIES", "AVG_EXECUTION_TIME", "CLUSTER_NUMBER"]),
        #alt.Tooltip( "AVG_EXECUTION_TIME:O", format=",.2f"),
        #tooltip=[ 'AVG_EXECUTION_TIME', alt.Tooltip('AVG_EXECUTION_TIME:Q', format='.1%')]
        #alt.Text('AVG_EXECUTION_TIME', format='%'), #,.1f
        
    )
    .interactive()
    ).properties(description='chart', width=1500, height=800)
        st.altair_chart(chart, theme=None, use_container_width=True)

    #########################

    tab1, tab2 = st.tabs(["With Cluster and Queued Time", "Queries Only"])

    with tab1:
        chart = (
    alt.Chart(df1, title="Queries in Cluster by Quing Time")
    .mark_circle()
    .encode(
        alt.X("WAREHOUSE_NAME:O", sort="descending"),
        alt.Y("QUERIES"),
        alt.Color("CLUSTER_NUMBER"),
        alt.Size("MEDIAN_QUEUED_TIME"), #, sort="descending"
        alt.Tooltip(["WAREHOUSE_NAME", "QUERIES", "MEDIAN_QUEUED_TIME", "CLUSTER_NUMBER"]),
        #alt.Tooltip( "AVG_EXECUTION_TIME:O", format=",.2f"),
        #tooltip=[ 'AVG_EXECUTION_TIME', alt.Tooltip('AVG_EXECUTION_TIME:Q', format='.1%')]
        #alt.Text('AVG_EXECUTION_TIME', format='%'), #,.1f
        
    )
    .interactive()
    ).properties(description='chart', width=1500, height=800)
        st.altair_chart(chart, theme="streamlit", use_container_width=True)
    with tab2:
        chart = (
    alt.Chart(df1, title="Queries in Cluster by Quing Time")
    .mark_circle()
    .encode(
        alt.X("WAREHOUSE_NAME:O", sort="descending"),
        alt.Y("QUERIES"),
        #alt.Color("CLUSTER_NUMBER"),
        #alt.Size("MEDIAN_QUEUED_TIME"), #, sort="descending"
        alt.Tooltip(["WAREHOUSE_NAME", "QUERIES", "MEDIAN_QUEUED_TIME", "CLUSTER_NUMBER"]),
        #alt.Tooltip( "AVG_EXECUTION_TIME:O", format=",.2f"),
        #tooltip=[ 'AVG_EXECUTION_TIME', alt.Tooltip('AVG_EXECUTION_TIME:Q', format='.1%')]
        #alt.Text('AVG_EXECUTION_TIME', format='%'), #,.1f
        
    )
    .interactive()
    ).properties(description='chart', width=1500, height=800)
        st.altair_chart(chart, theme=None, use_container_width=True)
    #st.altair_chart(chart, theme=None)
    #df1
    
    st.dataframe(df1)

else:
    st.info('Select an **Account** from the select box in the left sidebar.')