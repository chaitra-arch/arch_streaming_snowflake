import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.io as pio
import altair as alt
#from streamlit import caching

#from PIL import Image

#import duckdb

#pio.templates

#from pandasql import sqldf

from common import whitebear as wb


# ----------------------------------------------------------------------
# Side Bar Menu Options 
# ----------------------------------------------------------------------
with st.sidebar: 
    account_selected     = wb.account_selector()
    start_date, end_date = wb.date_selector()
    db_name = wb.account_db()
    sc_name = wb.account_schema()

# Pate Setup 
wb.page_setup('WH Full Analysis - Start Date: ' + str(start_date) + ' to  End Date: ' + str(end_date))
st.write('Need to have permission to create tables in Database and Schema')
st.write('PROVIDE db_name:' + str(db_name))
st.write('PROVIDE sc_name:' + str(sc_name))
# ----------------------------------------------------------------------
# Execute Query and Display Results 
# ----------------------------------------------------------------------

st.write('WH Full Analysis', 'This report may run a few minutes, the process time is depending on the selected data range and the current WH size.')

df_wh = wb.query('SN-WH-85-analysis-q0.sql',
                  {
                     'start_date': start_date,
                     'end_date'  : end_date,
                    
                  }
                )#get_df0()

wh_selected = ""   
#st.session_state.disabled = True
if "visibility" not in st.session_state:
    st.session_state.visibility = "visible"
    st.session_state.disabled = False

col1, col2 = st.columns(2)

with col1:
    exclude = st.checkbox("Exclude WHs", key="disabled")
    st.write('exclude: ' + str(exclude))
    #st.radio(
    #    "Set selectbox label visibility 👉",
    #    key="visibility",
    #    options=["visible", "hidden", "collapsed"],
    #)
    print('test')

with col2:
    #wh_selected = st.selectbox('Select to exclude WHs',df_wh['WAREHOUSE_NAME'] , label_visibility=st.session_state.visibility,
    #    disabled=False) #st.session_state.disabled,) #) + ' MONTH_CREDITS(' + str(df0['CREDITS'][1])+ ')')
    #st.write('You selected:', wh_selected)
    
    options = st.multiselect('Select to exclude WHs',df_wh['WAREHOUSE_NAME'], label_visibility=st.session_state.visibility, disabled= False ) #st.session_state.disabled,)
    

    option_s = "','".join(options)


    st.write('You selected:', option_s)

    st.write('You selected:', options)



if account_selected and db_name and sc_name:
   
    st.snow()

    df0 = wb.query('SN-WH-85-analysis-q00.sql',
                  {
                     'start_date': start_date,
                     'end_date'  : end_date,
                     'db_name' : db_name,
                     'sc_name' : sc_name,
                     'exclude' : exclude,
                     'wh_selected' : wh_selected
                     #'WAREHOUSE_NAME_IN' : option_s
                  }
                )#get_df()

    df = wb.query('SN-WH-85-analysis-q1.sql',
                  {
                     'start_date': start_date,
                     'end_date'  : end_date,
                     'db_name' : db_name,
                     'sc_name' : sc_name,
                     'exclude' : exclude,
                     'WAREHOUSE_NAME_IN' : option_s,
                     'wh_selected' : wh_selected
                  }
                )#get_df()
    #df_test = duckdb.query("SELECT WH_AND_SIZE, warehouse_name FROM df where warehouse_size = 'Large'").df()

    df_2a = wb.query('SN-WH-85-analysis-q2a.sql',
                  {
                     'start_date': start_date,
                     'end_date'  : end_date,
                     'db_name' : db_name,
                     'sc_name' : sc_name,
                     'exclude' : exclude,
                     'WAREHOUSE_NAME_IN' : option_s,
                     #'wh_selected' : wh_selected
                  }
                )#get_df()

    df_recommend = wb.query('SN-WH-85-analysis-q2.sql',
                  {
                     'start_date': start_date,
                     'end_date'  : end_date,
                     'db_name' : db_name,
                     'sc_name' : sc_name,
                     'exclude' : exclude,
                     'WAREHOUSE_NAME_IN' : option_s,
                     #'wh_selected' : wh_selected
                  }
                )#get_df()
    
    #caching.clear_cache()
    df_cost2 = wb.query('SN-WH-85-analysis-q3a.sql',
                  {
                     'start_date': start_date,
                     'end_date'  : end_date,
                     'db_name' : db_name,
                     'sc_name' : sc_name,
                     'exclude' : exclude,
                     'WAREHOUSE_NAME_IN' : option_s,
                     #'wh_selected' : wh_selected
                  }
                )#get_df()
    #caching.clear_cache()
    df_cost1 = wb.query('SN-WH-85-analysis-q3b.sql',
                  {
                     'start_date': start_date,
                     'end_date'  : end_date,
                     'db_name' : db_name,
                     'sc_name' : sc_name,
                     'exclude' : exclude,
                     'WAREHOUSE_NAME_IN' : option_s,
                     #'wh_selected' : wh_selected
                  }
                )#get_df()
    #caching.clear_cache()
    df_cost = wb.query('SN-WH-85-analysis-q3c.sql',
                  {
                     'start_date': start_date,
                     'end_date'  : end_date,
                     'db_name' : db_name,
                     'sc_name' : sc_name,
                     'exclude' : exclude,
                     'WAREHOUSE_NAME_IN' : option_s,
                     #'wh_selected' : wh_selected
                  }
                )#get_df()
    
    
    #duckdb.query("SELECT * FROM nyc where trip_distance>10").df()
    ''' '''
    st.text('Identified WHs with big difference on number of running queries in running time ranges (buckets) \nFind the WHs may be still not optimally resized')
    fig2 = px.bar(df, x='WH_AND_SIZE', y='COUNT_QUERIES', color = 'QS_PER_TIME',  title='WH Timing Buckets')
    #fig2.update_layout(barmode='stack', xaxis={'categoryorder':'total descending'})
    fig2.update_layout(barmode='relative')
    st.plotly_chart(fig2, template="plotly_white",use_container_width=True)
    st.divider()

    st.text('Identify queries in the WH running more than one hour \nCheck WHs why they are running too SMALL queries')
    
    fig3 = px.bar(df, x='WH_AND_SIZE', y='AVG_ALL_EXE_TIME', color = 'QS_PER_TIME', title='WH AVG_ALL_EXE_TIME (Seconds)')
    #fig3.update_layout(barmode='stack', xaxis={'categoryorder':'total ascending'})
    fig3.update_layout(barmode='relative')
    st.plotly_chart(fig3, template="plotly_white",use_container_width=True)
    st.divider()

    st.text('Identify queries in the WH running more than one hour \nCheck WHs running SMALL workload \nCheck on "SMALL" to "MED" WH sizes – running queries mor than 5+ minutes')
    
    fig3 = px.bar(df, x='WH_AND_SIZE', y='PROCESS_LOADED', color = 'QS_PER_TIME', title='WH PROCESS_LOADED AVG Percent')
    #fig3.update_layout(barmode='stack', xaxis={'categoryorder':'total ascending'})
    fig3.update_layout(barmode='relative')
    st.plotly_chart(fig3, template="plotly_white",use_container_width=True)
    st.divider()

    st.text('For WHs with minimal scanning consider WH downsizing \nCheck queries on WHs running higher  scanning')
    
    # For WHs with minimal scanning consider WH downsizing
    fig3 = px.bar(df, x='WH_AND_SIZE', y='AVG_BYTES_SCANED_GB', color = 'QS_PER_TIME', title='WH AVG Scanning GB')
    #fig3.update_layout(barmode='stack', xaxis={'categoryorder':'total ascending'})
    fig3.update_layout(barmode='relative')
    st.plotly_chart(fig3, template="plotly_white",use_container_width=True)

    fig3 = px.bar(df, x='WH_AND_SIZE', y='PERCENT_LARGE', color = 'QS_PER_TIME', title='RERCENT - WH Timinig on Larger Scanning')
    #fig3.update_layout(barmode='stack', xaxis={'categoryorder':'total ascending'})
    fig3.update_layout(barmode='relative')
    st.plotly_chart(fig3, template="plotly_white",use_container_width=True)

    


    fig3 = px.bar(df, x='WH_AND_SIZE', y='PERCENT_SMALL', color = 'QS_PER_TIME', title='PERCENT - WH Timinig on Smaller Scanning')
    #fig3.update_layout(barmode='stack', xaxis={'categoryorder':'total ascending'})
    fig3.update_layout(barmode='relative')
    st.plotly_chart(fig3, template="plotly_white",use_container_width=True)


    ##fig3 = px.bar(df, x='WH_AND_SIZE', y=['PER_NUM_JOBS_LOCAL_SPILLING', 'PER_NUM_JOBS_REMOTE_SPILLING'], color = 'QS_PER_TIME', title='WH Spilling Percenitle')
    #fig3.update_layout(barmode='stack', xaxis={'categoryorder':'total ascending'})
    ##fig3.update_layout(barmode='relative')
    ##st.plotly_chart(fig3, template="plotly_white",use_container_width=True)

    fig3 = px.bar(df, x='WH_AND_SIZE', y=['IS_LOCAL_SPILLING','IS_REMOTE_SPILLING'],  title='WH SPILLING')
    #fig3.update_layout(barmode='stack', xaxis={'categoryorder':'total ascending'})
    fig3.update_layout(barmode='relative')
    st.plotly_chart(fig3, template="plotly_white",use_container_width=True)
    # Memo: if too many BIG WHs in l3ss 1 min oprations; OR Small WH in long Timing
    #
    fig3 = px.bar(df, x='WH_AND_SIZE', y=['IS_LOCAL_SPILLING','IS_REMOTE_SPILLING'],  color = 'QS_PER_TIME', title='WH Sized SPILLING')
    #fig3.update_layout(barmode='stack', xaxis={'categoryorder':'total ascending'})
    fig3.update_layout(barmode='relative')
    st.plotly_chart(fig3, template="plotly_white",use_container_width=True)
    # Memo: if too many BIG WHs in l3ss 1 min oprations; OR Small WH in long Timing
    #  

    fig3 = px.bar(df, x='QS_PER_TIME', y=['PERCENT_SMALL','PERCENT_LARGE'],  color = 'WAREHOUSE_SIZE', title='WH Sized Sliced SCANNING by Timing Buckets 1')
    #fig3.update_layout(barmode='stack', xaxis={'categoryorder':'total ascending'})
    fig3.update_layout(barmode='relative')
    st.plotly_chart(fig3, template="plotly_white",use_container_width=True)
    # Memo: if too many BIG WHs in l3ss 1 min oprations; OR Small WH in long Timing
    # 
    fig3 = px.bar(df, x='WAREHOUSE_SIZE', y=['PERCENT_SMALL','PERCENT_LARGE'],  color = 'QS_PER_TIME', title='WH Sized Sliced SCANNING by Timing Buckets 2')
    #fig3.update_layout(barmode='stack', xaxis={'categoryorder':'total ascending'})
    fig3.update_layout(barmode='relative')
    st.plotly_chart(fig3, template="plotly_white",use_container_width=True)
    # Memo: if too many BIG WHs in l3ss 1 min oprations; OR Small WH in long Timing
    # 

    fig3 = px.bar(df, x='QS_PER_TIME', y='COUNT_QUERIES',  color = 'WAREHOUSE_SIZE', title='WH Sized Sliced Query COUNTS by Timing Buckets 1')
    #fig3.update_layout(barmode='stack', xaxis={'categoryorder':'total ascending'})
    fig3.update_layout(barmode='relative')
    st.plotly_chart(fig3, template="plotly_white",use_container_width=True)
    # Memo: if too many BIG WHs in l3ss 1 min oprations; OR Small WH in long Timing
    # 

    fig3 = px.bar(df, x='WAREHOUSE_SIZE', y='COUNT_QUERIES',  color = 'WAREHOUSE_SIZE', title='WH Sized Sliced Query COUNTS by Timing Buckets 2')
    #fig3.update_layout(barmode='stack', xaxis={'categoryorder':'total ascending'})
    fig3.update_layout(barmode='relative')
    st.plotly_chart(fig3, template="plotly_white",use_container_width=True)
    # Memo: if too many BIG WHs in l3ss 1 min oprations; OR Small WH in long Timing
    # 
    
 
    #fig3 = px.bar(df, x='WH_AND_SIZE', y='AVG_BYTES_LARGE_GB', color = 'WAREHOUSE_SIZE', title='WH Workload by Bytes')
    #fig3.update_layout(barmode='stack', xaxis={'categoryorder':'total ascending'})
    #fig3.update_layout(barmode='relative')
    #st.plotly_chart(fig3, template="plotly_white",use_container_width=True)
    st.write("Recommendations for WHs")
    chart = (
    alt.Chart(df_recommend)
    .mark_bar()
    .encode(
        alt.X("WH_AND_SIZE:O", sort="descending"),
        alt.Y("CNT_RECOMMENDATIONS"),
        alt.Color("WAREHOUSE_SIZE"),
        #alt.Size("PERCENT_LARGE"),
        alt.Tooltip(["WH_AND_SIZE","RECOMMENDATIONS"]),
        alt.Text('CNT_RECOMMENDATIONS:Q'), #, format='.1f'
        #alt.Order("WH_AND_SIZE", sort="ascending"),
    ).interactive()
    ).properties(description='This is a simple chart', width=1500, height=800)

    #st.altair_chart(chart, theme="streamlit", use_container_width=True)
    #line =  chart.mark_line(color='red').encode(
    #y='AVG_BYTES_LARGE:Q'
    #)

    st.altair_chart(chart)
    ##################
    st.write("Percentage of Large Scanning per Time Buckets")
    chart = (
    alt.Chart(df)
    .mark_circle()
    .encode(
        alt.X("WH_AND_SIZE:O", sort="descending"),
        alt.Y("COUNT_QUERIES"),
        alt.Color("QS_PER_TIME"),
        alt.Size("PERCENT_LARGE"),
        alt.Tooltip(["WH_AND_SIZE", "PERCENT_LARGE", "COUNT_QUERIES"]),
        alt.Text('COUNT_QUERIES:Q', format='.1f'),
    )
    .interactive()
    ).properties(description='This is a simple chart', width=1500, height=800)

    #st.altair_chart(chart, theme="streamlit", use_container_width=True)
    #line =  chart.mark_line(color='red').encode(
    #y='AVG_BYTES_LARGE:Q'
    #)

    st.altair_chart(chart)
    #####################
    st.write("Percentage of Processing per Time Buckets")
    st.write("The approximate percentage of active compute resources in the warehouse for the query execution.")
    chart1 = (
    alt.Chart(df)
    .mark_circle()
    .encode(
        alt.X("WH_AND_SIZE:O", sort="descending"),
        alt.Y("COUNT_QUERIES"),
        alt.Color("QS_PER_TIME"),
        alt.Size("PROCESS_LOADED"),
        alt.Tooltip(["WH_AND_SIZE", "PROCESS_LOADED","COUNT_QUERIES"]),
        alt.Text('COUNT_QUERIES:Q', format='.1f'),
    )
    .interactive()
    ).properties(description='This is a simple chart', width=1500, height=800)

    st.altair_chart(chart1)
    ##################### 
    ### st.write("Percentile of Local Spilling per Time Buckets")
    ### chart2 = (
    ### alt.Chart(df)
    ### .mark_circle()
    ### .encode(
    ###     alt.X("WH_AND_SIZE:O", sort="descending"),
    ###     alt.Y("PER_NUM_JOBS_LOCAL_SPILLING"),
    ###     alt.Color("QS_PER_TIME"),
    ###     alt.Size("PROCESS_LOADED"),
    ###     alt.Tooltip(["WH_AND_SIZE","PROCESS_LOADED","COUNT_QUERIES"]),
    ###     alt.Text('COUNT_QUERIES:Q', format='.1f'),
    ### )
    ### .interactive()
    ### ).properties(description='This is a simple chart', width=1500, height=800)

    ### st.altair_chart(chart2)

    ##################### PER_NUM_JOBS_LOCAL_SPILLING
    ''' 
    st.write("Percentile of Local Spilling per Time Buckets")
    chart2 = (
    alt.Chart(df)
    .mark_circle()
    .encode(
        alt.X("WH_AND_SIZE:O", sort="descending"),
        alt.Y("PER_NUM_JOBS_LOCAL_SPILLING"),
        alt.Color("QS_PER_TIME"),
        alt.Size("PROCESS_LOADED"),
        alt.Tooltip(["WH_AND_SIZE","PROCESS_LOADED","COUNT_QUERIES"]),
        alt.Text('COUNT_QUERIES:Q', format='.1f'),
    )
    .interactive()
    ).properties(description='This is a simple chart', width=1500, height=800)

    st.altair_chart(chart2)
    '''
    #### REcommendations

    

    #fig2 = px.bar(df, x='query_text', y='QUEUE_BUCKET', color='WAREHOUSE_NAME', title='Average All Query Exec Time for Loading by Warehouse')
    #st.plotly_chart(fig2, template="plotly_white",use_container_width=True)

    #image = Image.open('resources/wh_sizing_rule.png')

    #st.image(image, caption='Warehouse Sizing Rules')

    #st.dataframe(df2)
    st.table(df_recommend)
    st.table(df_cost)
    #st.dataframe(df_cost)
    #st.dataframe(df_recommend, width=1500, use_container_width=1)
    
    st.dataframe(df)
else:
    st.info('Select an **Account** , DB and SCHEMA from the select box in the left sidebar.')