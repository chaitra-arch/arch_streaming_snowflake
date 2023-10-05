import streamlit as st

from common import whitebear as wb

# Pate Setup 
wb.page_setup('<App Name>')

# ----------------------------------------------------------------------
# Side Bar Menu Options 
# ----------------------------------------------------------------------
with st.sidebar: 
    account_selected     = wb.account_selector()
    start_date, end_date = wb.date_selector()
    disable              = False if account_selected else True
    refresh_btn          = st.button('Refresh',disabled=disable)
    
# ----------------------------------------------------------------------
# Execute Query and Display Results 
# ----------------------------------------------------------------------
if refresh_btn:
    st.session_state.connection = account_selected['connection']

    df = wb.query('template.sql',
                  {
                   'start_date': start_date,
                   'end_date'  : end_date,
                   'timezone'  : account_selected['timezone']
                  }
                )

    st.dataframe(df)
else:
    st.info('Select an **Account** from the select box in the left sidebar.')