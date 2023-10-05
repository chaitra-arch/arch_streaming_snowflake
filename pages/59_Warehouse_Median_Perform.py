import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.io as pio
import altair as alt

from common import whitebear as wb

# Pate Setup 
wb.page_setup('Warehouse Median Performance in 30 Days')

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
    df = wb.query('SN-WH-median-perform.sql',
                  {
                    'start_date': start_date,
                    'end_date'  : end_date,
                  }
                )

    fig2 = px.bar(df, x='WAREHOUSE_NAME', y='AVG_QUERY_TIME' , color='WAREHOUSE_NAME', title='Warehouse AVG Query Time')
    fig2.update_layout(barmode='stack', xaxis={'categoryorder':'total descending'})
    fig2.update_layout(barmode='relative')
    st.plotly_chart(fig2, theme="streamlit", use_container_width=True)

    circle = (
    alt.Chart(df)
    .mark_circle()
    .encode(
        alt.X("WAREHOUSE_NAME:O"),
        alt.Y("AVG_QUERY_TIME", sort='y'),
        alt.Color("WAREHOUSE_SIZE"),
        alt.Size("MEDIAN_QUERY_TIME"),
        alt.Tooltip(["WAREHOUSE_NAME"]),
        alt.Text('CREDITS_USED:Q', format='.1f'),
    )
    .interactive()
    )

    rule = alt.Chart(df).mark_rule(color='red').encode(
        y='mean(AVG_QUERY_TIME):Q'
    )

    chart = (circle + rule).properties(width=600)

    st.altair_chart(chart, theme="streamlit", use_container_width=True)

    circle1 = (
    alt.Chart(df)
    .mark_circle()
    .encode(
        alt.X("WAREHOUSE_NAME:O"),
        alt.Y("AVG_QUERY_TIME", sort='y'),
        alt.Color("WAREHOUSE_SIZE"),
        #alt.Size("MEDIAN_QUERY_TIME"),
        alt.Tooltip(["WAREHOUSE_NAME"]),
        alt.Text('CREDITS_USED:Q', format='.1f'),
    )
    .interactive()
    )

    rule1 = alt.Chart(df).mark_rule(color='red').encode(
        y='mean(AVG_QUERY_TIME):Q'
    )

    chart1 = (circle1 + rule1).properties(width=600)

    st.altair_chart(chart1, theme="streamlit", use_container_width=True)
    #line =  chart.mark_rule(color='red').encode(
   # y='AVG_BYTES_LARGE:Q'
    #)


    st.dataframe(df)
else:
    st.info('Select an **Account** from the select box in the left sidebar.')