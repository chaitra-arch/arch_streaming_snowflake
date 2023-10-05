import streamlit as st

from common import whitebear as wb

# Pate Setup 
wb.page_setup('Query History')

# st.info('Select an **Account** from the select box in the left sidebar.')
# ----------------------------------------------------------------------
# Side Bar Menu Options 
# ----------------------------------------------------------------------
with st.sidebar:
    account_selected = wb.account_selector() 
    option           = st.selectbox("Query History by", ("Current Session", "Current User", "All"))
    disable          = False if account_selected else True
    refresh_btn      = st.button('Refresh',disabled=disable)

# ----------------------------------------------------------------------
# Execute Query and Display Results 
# ----------------------------------------------------------------------
if refresh_btn:
    st.session_state.connection = account_selected['connection']
    df = wb.run_query('select * from table(snowflake.information_schema.query_history_by_session()) order by start_time',)

    st.dataframe(df)
else:
    st.info('Select an **Account** from the select box in the left sidebar.')
