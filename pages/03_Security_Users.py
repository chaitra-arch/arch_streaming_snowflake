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
wb.page_setup('LOGIN_USER')

# ----------------------------------------------------------------------
# Side Bar Menu Options 
# ----------------------------------------------------------------------
with st.sidebar: 
    account_selected     = wb.account_selector()
    start_date, end_date = wb.date_selector()
    db_name = wb.account_db()
    sc_name = wb.account_schema()
    npf_name = wb.account_network_policy_filter()

# ----------------------------------------------------------------------
# Execute Query and Display Results 
# ----------------------------------------------------------------------
if account_selected:


    df4 = wb.query('SN-WH-10-securityusers-q4.sql',
                  {
                     'start_date': start_date,
                     'end_date'  : end_date
                  }
                )#get_df()
    st.dataframe(df4)
    df = wb.query('SN-WH-10-securityusers-q1.sql',
                  {
                     'start_date': start_date,
                     'end_date'  : end_date
                  }
                )#get_df()

    df2 = wb.query('SN-WH-10-securityusers-q2.sql',
                  {
                     'start_date': start_date,
                     'end_date'  : end_date
                  }
                )#get_df()
    df3 = wb.query('SN-WH-10-securityusers-q3.sql',
                  {
                     'start_date': start_date,
                     'end_date'  : end_date
                  }
                )#get_df()
    st.dataframe(df3)

    
    df6 = wb.query('SN-WH-10-securityusers-q6.sql',
                  {
                     'start_date': start_date,
                     'end_date'  : end_date
                  }
                )#get_df()
    df7 = wb.query('SN-WH-10-securityusers-q7.sql',
                  {
                     'start_date': start_date,
                     'end_date'  : end_date
                  }
                )#get_df()
    df8 = wb.query('SN-WH-10-securityusers-q8.sql',
                  {
                     'start_date': start_date,
                     'end_date'  : end_date
                  }
                )#get_df()
    
    fig1 = px.bar(df, x='USER_NAME', y='ERROR_MESSAGE', color = 'ERROR_MESSAGE',  title='ERRORS LOGIN USER')
    fig1.update_layout(barmode='stack', xaxis={'categoryorder':'total ascending'})
    fig1.update_layout(barmode='relative')
    st.plotly_chart(fig1, template="plotly_white",use_container_width=True)

    fig2 = px.bar(df, x='USER_NAME', y='NUM_OF_FAILURES', color = 'ERROR_MESSAGE',  title='ERRORS LOGIN USER')
    fig2.update_layout(barmode='stack', xaxis={'categoryorder':'total ascending'})
    fig2.update_layout(barmode='relative')
    st.plotly_chart(fig2, template="plotly_white",use_container_width=True)


    fig2 = px.bar(df2, x='ERROR_MESSAGE', y='NUM_OF_FAILURES', color = 'ERROR_MESSAGE',  title='ERRORS LOGIN USER')
    fig2.update_layout(barmode='stack', xaxis={'categoryorder':'total ascending'})
    fig2.update_layout(barmode='relative')
    st.plotly_chart(fig2, template="plotly_white",use_container_width=True)

    fig3 = px.bar(df3, x='USER_NAME', y='CNT', color = 'AUTHENTICATION_METHOD',  title='PASSWORD VS DUO LOGIN USER')
    fig3.update_layout(barmode='stack', xaxis={'categoryorder':'total ascending'})
    fig3.update_layout(barmode='relative')
    st.plotly_chart(fig3, template="plotly_white",use_container_width=True)


    fig2 = px.bar(df7, x='USER_NAME', y='CNT', color = 'REPORTED_CLIENT_TYPE',  title=' LOGIN USER BY TYPE')
    fig2.update_layout(barmode='stack', xaxis={'categoryorder':'total ascending'})
    fig2.update_layout(barmode='relative')
    st.plotly_chart(fig2, template="plotly_white",use_container_width=True)

    fig3 = px.bar(df8, x='REPORTED_CLIENT_TYPE', y='CNT', color = 'REPORTED_CLIENT_TYPE',  title='LOGGIN TYPES')
    fig3.update_layout(barmode='stack', xaxis={'categoryorder':'total ascending'})
    fig3.update_layout(barmode='relative')
    st.plotly_chart(fig3, template="plotly_white",use_container_width=True)
    
    

    #if 1 == 1 : #account_selected and db_name and sc_name and npf_name:
    if account_selected and db_name and sc_name and npf_name:  

      df5a = wb.query('SN-WH-10-securityusers-q5a.sql',
                  {
                     'start_date': start_date,
                     'end_date'  : end_date,
                     'db_name' : db_name,
                     'sc_name' : sc_name,
                     'npf_name' : npf_name
                  }
                )#get_df()
      
      df5 = wb.query('SN-WH-10-securityusers-q5.sql',
                  {
                     'start_date': start_date,
                     'end_date'  : end_date,
                     'db_name' : db_name,
                     'sc_name' : sc_name,
                     'npf_name' : npf_name
                  }
                )#get_df()
    
      fig5 = px.bar(df5, x='USER_NAME_POLICY', y='CNT_IP', color = 'CLIENT_IP',  title='EXCLUDING/FILTERING PRIVATE LOGINs')
      fig5.update_layout(barmode='stack', xaxis={'categoryorder':'total ascending'})
      fig5.update_layout(barmode='relative')
      st.plotly_chart(fig5, template="plotly_white",use_container_width=True)

      df5
    else:
      st.info('Select an **Account** , DB and SCHEMA and NP Filter from the select box in the left sidebar.')

    fig6 = px.bar(df6, x='REPORTED_CLIENT_TYPE', y='LOGIN_FAILURE_RATE', color = 'REPORTED_CLIENT_TYPE',  title='CLIENT TYPE FAILED LOGINs')
    fig6.update_layout(barmode='stack', xaxis={'categoryorder':'total ascending'})
    fig6.update_layout(barmode='relative')
    st.plotly_chart(fig6, template="plotly_white",use_container_width=True)

    fig6 = px.bar(df6, x='REPORTED_CLIENT_TYPE', y=['FAILED_LOGINS', 'LOGINS'], color = 'LOGIN_FAILURE_RATE',  title='CLIENT TYPE FAILED LOGINs')
    fig6.update_layout(barmode='stack', xaxis={'categoryorder':'total ascending'})
    fig6.update_layout(barmode='relative')
    st.plotly_chart(fig6, template="plotly_white",use_container_width=True)


    fig7 = px.bar(df7, x='USER_NAME', y='CNT', color = 'REPORTED_CLIENT_TYPE',  title='USER TYPE  LOGINs')
    fig7.update_layout(barmode='stack', xaxis={'categoryorder':'total ascending'})
    fig7.update_layout(barmode='relative')
    st.plotly_chart(fig7, template="plotly_white",use_container_width=True)

    fig8 = px.bar(df8, x='REPORTED_CLIENT_TYPE', y='CNT', color = 'REPORTED_CLIENT_TYPE',  title='CLIENT TYPE  LOGINs')
    fig8.update_layout(barmode='stack', xaxis={'categoryorder':'total ascending'})
    fig8.update_layout(barmode='relative')
    st.plotly_chart(fig8, template="plotly_white",use_container_width=True)


    
    
    #st.dataframe(df)
else:
    st.info('Select an **Account** from the select box in the left sidebar.')