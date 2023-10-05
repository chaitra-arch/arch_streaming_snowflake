import streamlit as st
import pandas as pd

from common import whitebear as wb

# Page Setup
wb.page_setup('Warehouse Availability')


# ----------------------------------------------------------------------
# Side Bar Menu Options 
# ----------------------------------------------------------------------
with st.sidebar: 
    account_selected     = wb.account_selector()
    disable = False if account_selected else True
    refresh_btn = st.button('Refresh',disabled=disable)

# ----------------------------------------------------------------------
# Execute Query and Display Results 
# ----------------------------------------------------------------------
if refresh_btn:
    st.session_state.connection = account_selected['connection']

    df = wb.run_query('show warehouses')

    # Select current warehouse
    row = df.loc[df['is_current'] == 'Y']

    # Display the value of column 'B' from the selected row
    st.markdown(f"Current Warehouse: {row['name'].values[0]}")

    col1, col2, col3, col4, col5 = st.columns(5)

    col1.metric(label="Size", value=row['size'].values[0] )
    col2.metric(label="Max Clusters", value=row['max_cluster_count'].values[0] )
    col3.metric(label="Started Clusters", value=row['started_clusters'].values[0] )
    col4.metric(label="Running SQLs", value=row['running'].values[0] )
    col5.metric(label="Queued", value=row['queued'].values[0] )
    wb.style_metric_cards()

    st.dataframe(df)

else:
    st.info('Select an **Account** from the select box in the left sidebar.')