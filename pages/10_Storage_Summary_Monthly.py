import streamlit as st
import pandas as pd
import plotly.express as px

from common import whitebear as wb

# Pate Setup 
wb.page_setup('Storage Summary by Month')

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

    df = wb.query('SN-storage-summary.sql',
                  {
                     'start_date': start_date,
                     'end_date'  : end_date,
                  }
                )

    fig2 = px.bar(df, x='USAGE_MONTH', y=['STORAGE_TB', 'STAGE_TB','FAILSAFE_TB'], title='Bytes')
    st.plotly_chart(fig2, theme="streamlit", use_container_width=True)

    st.dataframe(df)
else:
    st.info('Select an **Account** from the select box in the left sidebar.')