import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.io as pio
import altair as alt

#import duckdb

from PIL import Image

#pio.templates

from common import whitebear as wb

# Pate Setup 
wb.page_setup('Virtual Warehouse Clusters')

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

    #get_df0()
    df0 = wb.query('SN-WH-83-clusters-q0.sql', 
                  {
                     'start_date': start_date,
                     'end_date'  : end_date,
                  }
                )#get_df0()

    #duckdb.query("SELECT * FROM nyc where trip_distance>10").df()
    

    
    wh_selected = st.selectbox('Select a WH',df0['WAREHOUSE_NAME']) #) + ' MONTH_CREDITS(' + str(df0['CREDITS'][1])+ ')')

    st.write('You selected:', wh_selected)

    df = wb.query('SN-WH-86-wh_details-q1.sql',
                  {
                     'start_date': start_date,
                     'end_date'  : end_date,
                     'WAREHOUSE_NAME_IN'  : wh_selected,
                  }
                )#get_df()

    #fig2 = px.bar(df, x='TIME_SEC', y='MAX_JOB_ACTIVE_COUNT_MS', color = 'CLUSTER_NUMBER',  title='WH MAX_JOB_ACTIVE_COUNT in MINUTE')
    #fig2.update_layout(barmode='stack', xaxis={'categoryorder':'total ascending'})
    #fig2.update_layout(barmode='relative')
    #st.plotly_chart(fig2, template="plotly_white",use_container_width=True)
    
    
    chart = (
    alt.Chart(df)
    .mark_circle()
    .encode(
        alt.X("QUERY_TYPE"), #alt.X("TIME_SEC:O"),
        alt.Y("AVG_TOTAL_ELAPSED_TIME_SEC"),
        alt.Color("QUERY_TYPE"),
        alt.Size("CNT_QURY_TYPE"),
        #alt.Tooltip(["WH_AND_SIZE", "AVG_BYTES_LARGE"]), AVG_JOB_ACTIVE_COUNT_MS
        alt.Text('AVG_TOTAL_ELAPSED_TIME_SEC:Q', format='.1f'),
    )
    .interactive()
    )

    st.altair_chart(chart, theme="streamlit", use_container_width=True)
    line =  chart.mark_line(color='red').encode(
    y='MAX_JOB_ACTIVE_COUNT_MS:Q'
    )

    #st.altair_chart(chart)

    #fig2 = px.bar(df, x='query_text', y='QUEUE_BUCKET', color='WAREHOUSE_NAME', title='Average All Query Exec Time for Loading by Warehouse')
    #st.plotly_chart(fig2, template="plotly_white",use_container_width=True)

    #image = Image.open('resources/wh_sizing_rule.png')

    #st.image(image, caption='Warehouse Sizing Rules')
    
    st.dataframe(df)
else:
    st.info('Select an **Account** from the select box in the left sidebar.')