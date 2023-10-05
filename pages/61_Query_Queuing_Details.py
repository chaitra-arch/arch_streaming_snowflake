import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.io as pio
#pio.templates

from common import whitebear as wb

# Pate Setup 
wb.page_setup('Query Queueing Details')

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

    @st.cache_data
    def get_df():
        df = wb.query('SN-query-queue-details.sql',
                  {
                     'start_date': start_date,
                     'end_date'  : end_date,
                  }
                )
        return df

    df = get_df()

    fig2 = px.histogram(df, x='RUN_DATE', y='QUEUED_TIME_SEC_AVG', barmode='group', title='Query Queueing Average for Account')
    st.plotly_chart(fig2, template="plotly_white",use_container_width=True)

    fig2 = px.bar(df, x='RUN_DATE', y='QUEUED_TIME_SEC_AVG', color='WAREHOUSE_NAME', title='Queueing Average by Warehouse')
    st.plotly_chart(fig2, template="plotly_white",use_container_width=True)

    fig2 = px.bar(df, x='RUN_DATE', y='ELAPSED_TIME_SEC_AVG', color='WAREHOUSE_NAME', title='Query Elapsed Time Average on Queueing Warehouse')
    st.plotly_chart(fig2, template="plotly_white",use_container_width=True)

    fig2 = px.bar(df, x='WAREHOUSE_NAME', y='QUEUED_TIME_SEC_AVG', title='Query Queuing Average by Warehouse Culmultive')
    st.plotly_chart(fig2, template="plotly_white",use_container_width=True)

    fig2 = px.bar(df, x='WAREHOUSE_NAME', y='QUEUED_TIME_SEC_AVG', title='Query Queuing Average by Warehouse Culmultive Sorted')
    fig2.update_layout(barmode='stack', xaxis={'categoryorder':'total ascending'})
    fig2.update_layout(barmode='relative')
    st.plotly_chart(fig2, template="plotly_white",use_container_width=True)

    
    option_wh = st.selectbox('Select Advertisers',df["WAREHOUSE_NAME"])

    st.write('You selected:', option_wh)

    @st.cache_data
    def get_df_wh():
        df1 = wb.query('SN-query-queue-details_sub1.sql',
                  {
                     'start_date': start_date,
                     'end_date'  : end_date,
                     'option_wh' : option_wh,
                  }
                )
        return df1

    df_wh = get_df_wh()

    fig2 = px.bar(df_wh, x='CLUSTER_NUMBER', y='CNT', color = 'WORKLOAD_GR', title='Query Workload Counts per Cluster')
    fig2.update_layout(barmode='stack', xaxis={'categoryorder':'total ascending'})
    fig2.update_layout(barmode='relative')
    st.plotly_chart(fig2, template="plotly_white",use_container_width=True)


    #fig2 = px.bar(df, x='query_text', y='QUEUE_BUCKET', color='WAREHOUSE_NAME', title='Average All Query Exec Time for Loading by Warehouse')
    #st.plotly_chart(fig2, template="plotly_white",use_container_width=True)


    #st.dataframe(df)
else:
    st.info('Select an **Account** from the select box in the left sidebar.')