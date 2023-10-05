import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.io as pio
import altair as alt

from PIL import Image

#pio.templates

from common import whitebear as wb

# Pate Setup 
wb.page_setup('Virtual Warehouse Load Analysis By Scanning Sizes')

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

    #@st.cache_data


    df = wb.query('SN-WH-83-scanning-view.sql',
                  {
                     'start_date': start_date,
                     'end_date'  : end_date,
                  }
                )#get_df()
    

    fig2 = px.bar(df, x='WH_AND_SIZE', y=['PERCENT_LARGE', 'PERCENT_SMALL'],  title='WH Workload Percentile')
    fig2.update_layout(barmode='stack', xaxis={'categoryorder':'total ascending'})
    fig2.update_layout(barmode='relative')
    st.plotly_chart(fig2, template="plotly_white",use_container_width=True)

    fig3 = px.bar(df, x='WH_AND_SIZE', y='AVG_BYTES_LARGE_GB', color = 'WAREHOUSE_SIZE', title='WH Workload by Bytes')
    fig3.update_layout(barmode='stack', xaxis={'categoryorder':'total ascending'})
    fig3.update_layout(barmode='relative')
    st.plotly_chart(fig3, template="plotly_white",use_container_width=True)

    fig3 = px.bar(df, x='WH_AND_SIZE', y='AVG_BYTES_LARGE_GB', color = 'WAREHOUSE_SIZE', title='WH Workload by Bytes')
    fig3.update_layout(barmode='stack', xaxis={'categoryorder':'total ascending'})
    fig3.update_layout(barmode='relative')
    st.plotly_chart(fig3, template="plotly_white",use_container_width=True)


    chart = (
    alt.Chart(df)
    .mark_circle()
    .encode(
        alt.X("WH_AND_SIZE:O"),
        alt.Y("AVG_BYTES_LARGE_GB"),
        alt.Color("WAREHOUSE_SIZE"),
        alt.Size("CREDITS_USED"),
        alt.Tooltip(["WH_AND_SIZE", "AVG_BYTES_LARGE", 'CREDITS_USED']),
        alt.Text('CREDITS_USED:Q', format='.1f'),
    )
    .interactive()
    )

    st.altair_chart(chart, theme="streamlit", use_container_width=True)
    line =  chart.mark_line(color='red').encode(
    y='AVG_BYTES_LARGE:Q'
    )                

    chart = (
    alt.Chart(df)
    .mark_circle()
    .encode(
        alt.X("WH_AND_SIZE:O"),
        alt.Y("AVG_ALL_EXE_TIME"),
        alt.Color("WAREHOUSE_SIZE"),
        alt.Size("CREDITS_USED"),
        alt.Tooltip(["WH_AND_SIZE", "AVG_ALL_EXE_TIME", 'CREDITS_USED']),
        alt.Text('CREDITS_USED:Q', format='.1f'),
    )
    .interactive()
    )

    st.altair_chart(chart, theme="streamlit", use_container_width=True)
    line =  chart.mark_line(color='red').encode(
    y='AVG_BYTES_LARGE:Q'
    )                        

    chart = (
    alt.Chart(df)
    .mark_circle()
    .encode(
        alt.X("WH_AND_SIZE:O"),
        alt.Y("AVG_ALL_EXE_TIME"),
        alt.Color("WAREHOUSE_SIZE"),
        #alt.Size("CREDITS_USED"),
        alt.Tooltip(["WH_AND_SIZE", "AVG_ALL_EXE_TIME", 'CREDITS_USED']),
        alt.Text('CREDITS_USED:Q', format='.1f'),
    )
    .interactive()
    )

    st.altair_chart(chart, theme="streamlit", use_container_width=True)
    line =  chart.mark_line(color='red').encode(
    y='AVG_BYTES_LARGE:Q'
    )                        

    #st.altair_chart(chart)

    #fig2 = px.bar(df, x='query_text', y='QUEUE_BUCKET', color='WAREHOUSE_NAME', title='Average All Query Exec Time for Loading by Warehouse')
    #st.plotly_chart(fig2, template="plotly_white",use_container_width=True)

    image = Image.open('resources/wh_sizing_rule.png')

    st.image(image, caption='Warehouse Sizing Rules')

    st.dataframe(df)
else:
    st.info('Select an **Account** from the select box in the left sidebar.')