import streamlit as st
import pandas as pd
import tomli
from streamlit_extras.app_logo import add_logo


secrets = tomli.load(open("streamlit/secrets.toml", "rb"))
connection_list = list(secrets.keys())

#Page Setup
st.set_page_config(
        page_title = 'Snowflake',
        layout     = 'wide',
        page_icon  = 'resources/favicon.png'
        )

add_logo("resources/logo.png", height=50)


# Required Session_State Variable 
if 'connection' not in st.session_state:
    st.session_state['connection'] = connection_list[0]
if 'debug' not in st.session_state:
    st.session_state['debug'] = False
if 'ttl' not in st.session_state:
    st.session_state['ttl'] = 600


# Load Account Information toml file
with open("data/accounts.toml", mode="rb") as fp:
     accounts = tomli.load(fp)

df = pd.DataFrame(accounts)

st.title("Snowflake Account Analysis")

st.dataframe(df)

# Global (session_state) variables 
st.session_state["debug"] = st.checkbox('Show source SQL in apps',value=st.session_state['debug'])
st.session_state['connection']  = st.selectbox('Account',connection_list, connection_list.index(st.session_state['connection']))
st.session_state['ttl']  = st.number_input('Time in seconds to cache query results',value=st.session_state['ttl'])


st.write(f'Streamlit Version: {st.__version__}')
