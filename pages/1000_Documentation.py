import streamlit as st
import pandas as pd

from common import whitebear as wb

wb.page_setup('WhiteBear')

st.markdown("""
The WhiteBear framework offers a streamlined solution for the development of interactive, multi-page data applications using Streamlit and Snowflake. 
It provides a comprehensive set of pre-built functions, configuration files, and sample scripts that simplify the process of connecting to Snowflake, 
executing SQL queries, visualizing results, and incorporating user inputs.

The framework has been designed to work with the Snowhouse database and Account_Usage schema, but its functionalities can be extended to other types of applications.
With the framework, SQL queries can be easily edited and reused as they are separated from the python code. This enables faster and more efficient development processes.

The current implementation of WhiteBear operates as a multi-page Streamlit application on a local computer. 
However, many of its functions can also be utilized within Streamlit in Snowsight, providing greater flexibility in deployment options. 
Overall, the WhiteBear framework represents a powerful tool for streamlining the development of data applications, 
reducing development time and increasing the efficiency of data analysis processes.
""")

st.markdown("""
- <a href="#directory-structure">Directory Structure</a>
- <a href="#config-files">Config Files</a>
- <a href="#session-state-variables">Session State Variables</a>
- <a href="#functions">Funtions</a>
""",unsafe_allow_html=True)

#########################################################################
# Directory Structure
#########################################################################
st.subheader('Directory Structure')


st.markdown("""
This directory structure organizes the codebase, data and config files into different sections.

| Directory   | Description                                                                                                                                                                                                                                                           |
| ----------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| - root -    | Home.py                                                                                                                                                                                                                                                               |
| .streamlit/ | Streamlit directory which contains the config.toml and secrets.toml files. The config.toml is optional (see [What is the path of Streamlit’s config.toml file? - Streamlit Docs](https://docs.streamlit.io/knowledge-base/using-streamlit/path-streamlit-config-toml)) the secrets.toml file is required.                                                                                                                                                                                                     |
| common/     | Contains the WhiteBear module, WhiteBear.py, which is a collection of python functions to be used your scripts.  Additional modules can be placed is this directory.                                                                                                   |
| data/       | Contains accounts.toml, which is used by the WhiteBear module.  Additionally, you can places any data files that maybe used by your scripts.                                                                                                                         |  
| pages/      | Contains streamlit scripts you create. Provided sripts include: (Documentation.py) a interactive documentation of WhiteBear functions, (test_connection.py) for testing connection to Snowflake and (template.py) that provdes a template for creating additional scripts. |
| resources/  | Contains any static resources such as images                                                                                                                                                                                                                                 |
| sql/        | Contains sql files that are used by (Steamlit) python apps you develope.                                                                                                     |

""")

#########################################################################
# Config Files
#########################################################################
st.subheader("Config Files")

st.markdown("""
Streamlit and WhiteBear uses TOML (Tom's Obvious, Minimal Language) file format for configuration files.
It is designed to be easy to read and write, and aims to provide a more human-friendly alternative to other configuration file formats such as JSON and XML. 
TOML files contain key-value pairs separated by an equal sign (=), and the structure of the data is defined using square brackets ([]). 
The values can be strings, integers, floating-point numbers, booleans, dates, and arrays of these types. 
""")

#-----------------------------------------------------------------------
# accounts.toml
#-----------------------------------------------------------------------
st.markdown("""
#### :blue[accounts.toml]
The *data/accounts.toml* file is utilized to store information regarding one or multiple Snowflake accounts. 
The values contained in this file serve as variables that are utilized in queries and Python scripts.  


 To access these values, the wb.account_sector() function is used. Upon selection of an account, a dictionary that contains all 
 values associated with the chosen account will be returned. If necessary, additional values can be added to this dictionary as required.

| **Value**    | **Type** | **Required** | **Description** |
| ------------ | ------ | -------------- | ------------------------------------------------------------ |
| description  | string |                |Description of account |
| environment  | string |                |Environment (Dev, QA, Prod, etc) |
| deployment   | string | Yes            |The deployment of the account (schema in Snowhouse).  (e.g. va2, prod, ie, awseast1, etc. ) |
| credit_cost  | float  | Yes            |Credit credit cost |
| storage_cost | float  | Yes            |Storage credit cost |
| timezone     | string | Yes            |Timezone for converting timestamp to the specified timezone.  e.g. America/Los_Angeles, America/New_York, Europe/Paris, etc. |
| connection   | string | Yes            |The connection name must match a key in the ./streamlit/secrets.toml file. |  


**Example of the data/accounts.toml file:** 
""",unsafe_allow_html=True)

st.code("""
[PROD ACCOUNT]
description  = "US Production"
environment  = "PROD"
deployment   = "va2"
credit_cost  = 2.80
storage_cost = 23.00
timezone     = "America/New_York"
connection   = "snowhouse"

[DEV ACCOUNT]
description  = "EU Developement"
environment  = "DEV"
deployment   = "ie"
credit_cost  = 3.64
storage_cost = 23.00
timezone     = "Europe/Paris"
connection   = "snowhouse"
""")

#-----------------------------------------------------------------------
# secrets.toml
#-----------------------------------------------------------------------
st.markdown("""
#### :blue[secrets.toml]
The .streamlit/secrets.toml file is a configuration file used by the Streamlit.  It contains the databases credentials needed to access Snowflake. 
The secrets in the file are stored as key-value pairs, with the keys being the names of the secrets and the values being the actual secrets themselves. 

For more information see: [Connect Streamlit to Snowflake - Streamlit Docs](https://docs.streamlit.io/knowledge-base/tutorials/databases/snowflake)

**Example:**   
""")

st.code("""
[snowhouse]
user          = "<user name>"
account       = "snowhouse"
authenticator = "externalbrowser"
warehouse     = "snowhouse"
role          = "technical_account_manager"

[cas2]
user      = "<user name>"
password  = "<password>"
account   = "aws_cas2"
warehouse = "<warehouse name>"
""")

#########################################################################
# Session State Variables
#########################################################################
st.subheader('Session State Variables')

st.markdown("""
Session state variables is a Streamlit feature that allows you to store and retrieve variables accross different runs and apps. 
The WhiteBear module requires 3 session state variables which must be set in the Home.py script.  

| **Variable** | **Description** |
| ------------ | ------------------------------------------------------------ |
| connection   | Default connection name (from .streamlit/secrets.toml file).  When the snowshovle.py module is opened, it uses this value to make a connection to Snowflake.  |
| debug        | Used in the wb.debug(text) function, when set to True the text passed to the function will be displayed in an streamlit expander.  The wb.query() function will display the SQL being run, in a streamlit expander, when set to True.  |
| ttl          | The maximum number of seconds to keep query results in the cache. The default is 600 (6 minutes).  This should be set to 0 when switching between Snowflake accounts.|


**Example:** 
""")

st.code("""
# Read the .streamlit/secrets.toml file and get a list of keys
secrets = tomli.load(open(".streamlit/secrets.toml", "rb"))
connection_list = list(secrets.keys())

# Required Session_State Variable 
if 'connection' not in st.session_state:
    st.session_state['connection'] = connection_list[0]
if 'debug' not in st.session_state:
    st.session_state['debug'] = False
if 'ttl' not in st.session_state:
    st.session_state['ttl'] = 600
""")

#########################################################################
# Queries
#########################################################################
st.subheader('Queries')

st.markdown("""

Queries are stored as separate text files in the sql/ directory. Queries variables are variables names surrounded by curly brackets, e.g. {start_date}.
Variables can be used anywhere in the query (schema name, table name, predicate value, column name, etc).  

Variables can be named anything, but to make apps and queries more reusable it is best to use a common set of variable names.  

| Variable Name | Description                                                                                                                       | Example                                                    |
| ------------- | --------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------- |
| {start_date}  | Start Date for use in a conditional expression                                                                                    | WHERE j.created_on BETWEEN '{start_date}' AND '{end_date}' |
| {end_date}    | End Date for use in a conditional expresion                                                                                       | WHERE j.created_on BETWEEN '{start_date}' AND '{end_date}' |
| {query_id}    | Query UUID, the Internal/system-generated identifier for a SQL statement                                                          | WHERE j.uuid = '{query_id}'                                |
| {user_name}   | User name used in a predicate                                                                                                     | WHERE j.user_name = '{user_name}'                          |
| {date}        | Date used in a predicate when a single date is need                                                                               | WHERE wl.timestamp = '{date}'                              |
| {warehouse}   | Warehouse name used in a predicate                                                                                                | WHERE warehouse_name = '{warehouse}'                       |
| {timezone}    | Used with the Snowflake CONVERT_TIMEZONE function                                                                                 | CONVERT_TIMEZONE('{timezone}', j.created_on)               |
| {sql_where}   | Used to add dynamically created WHERE conditions. In the example sql_where could be assinged a value of " AND user_name = 'Bob' " | WHERE A.NAME = '{account}' {sql_where}                     |
| {error_code}  | Used in a SQL predicate. Note that error_code is a numeric value so in the example it is not surrounded by single quote           | WHERE error_code = {error_code}                            |

  
In the example below the {deployment} variables is used for the schema name and {account}, {start_date} and {end_date} variables are used in predicates. 
""")

st.code("""
select error_code,
    any_value(error_message) AS error_message,
    count(*) AS query_count,
    sum(total_elapsed_time) AS total_elapsed_time
FROM snowflake.account_usage.query_history q
WHERE start_time BETWEEN '{start_date}' AND '{end_date}'
  AND error_code IS NOT NULL
GROUP BY error_code
ORDER BY total_elapsed_time desc
""")

st.markdown("""
A Steamlit (python) app sets the value for the variables then call the WhiteBear wb.query() function with the sql file name and a dictionary that maps the variable in the sql file to the variables in the python app.   

Below is a full Streamlit app that uses the WhiteBear module and the SQL file above.   This app does the following:  
- Imports the WhiteBear module
- Places a dropdown lists, in the sidebar,  for the the users to select an account and date range.
- Calls the wb.query function with the query file name to run and the variables.  The query functions connects to the database, runs the query and returns the results as a dataframe.
- Display the dataframe created.   
""")

st.code("""
import streamlit as st

from common import whitebear as wb

wb.page_setup('Query Errors Analysis')

# ----------------------------------------------------------------------
# Side Bar Menu Options 
# ----------------------------------------------------------------------
with st.sidebar: 
    start_date, end_date = wb.date_selector()
    refresh_btn = st.button('Refresh')

# ----------------------------------------------------------------------
# Execute Query and Display Results 
# ----------------------------------------------------------------------
if refresh_btn:

    df = wb.query('query_error_user.sql',
                  {
                   'start_date': start_date,
                   'end_date'  : end_date,
                  }
                )

    st.dataframe(df)
""")

#########################################################################
# Functions
#########################################################################
st.subheader('Functions')

st.markdown("""
The whitebear.py module, located in the common/ directory, is a collection of common functions for use in scripts.  To import this module in
all your script include the following at the top of your python scripts in the pages/ directory.   
Note that the whitebear.py requires 3 session state variables (connection, ttl and debug) be set in the Home.py script.
""")
st.code('from common import whitebear as wb')

#-----------------------------------------------------------------------
# account_selector()
#-----------------------------------------------------------------------
st.markdown("""
#### :blue[account_selector()]
Creates a dropdown list of accounts from the accounts.toml file. 
When the user selects an account, the function returns a dictionary of all the values associated with the account. 

**Dictionary Keys**   
account_selected['timezone']  
account_selected['customer']  
account_selected['description']  
account_selected['environment']  
account_selected['credit_cost']  
account_selected['storage_cost']  

**Example:**
""")

with st.echo():
    account_selected = wb.account_selector()
    if account_selected:
        st.write(f"{account_selected['sf_account'] =}")
        st.write(f"{account_selected['deployment'] = }")


#-----------------------------------------------------------------------
# convert_milliseconds(milliseconds, to_unit)
#-----------------------------------------------------------------------
st.markdown("""
#### :blue[convert_milliseconds(milliseconds, to_unit)]
Convert the given number of milliseconds to the specified unit.

**Parameters:**  
milliseconds (int): The number of milliseconds to be converted.  
to_unit (str): The unit to convert the milliseconds to. Default is 'seconds'. Accepted values are 'seconds', 'minutes', 'hours'.
               
**Returns:**  
float: The converted value in the specified unit.  -1 is returned if a 'Invalid unit' is specified not one of the accepted values.

**Example:** 
""")

with st.echo():
    milliseconds = st.number_input('Enter milliseconds', value=900000)
    min = wb.convert_milliseconds(milliseconds,'minutes')
    st.write(f'{milliseconds} milliseconds is {min} minutes')


#-----------------------------------------------------------------------
# date_selector()
#-----------------------------------------------------------------------
st.markdown("""
#### :blue[date_selector()]
This function provides a strimlit user interface to select a date range. It returns a tuple of two dates, the start date and the end date.
The function provides options to select a predefined date range, or a custom range by selecting the start and end date (using the streamlit calendar widgit).
It also includes an option to exclude today's date from the range. 

**Parameters:**  
-none- 

**Returns:**  
    tuple(star_date, end_date)

**Example:** 
""")

with st.echo():
    start_date, end_date = wb.date_selector()
    st.write(f'{start_date = :%Y-%m-%d} {end_date = :%Y-%m-%d}')

#-----------------------------------------------------------------------
# format_bytes(size_bytes: int)
#-----------------------------------------------------------------------
st.markdown("""
#### :blue[format_bytes(size_bytes)]
Formats a size in bytes to a string in KB, MB, GB, etc.

**Args:**  
    size_bytes (int): The size in bytes.

**Returns:**  
   str: The formatted size in KB, MB, GB, etc.
""")

with st.echo():
    bytes = st.number_input('Enter a number of bytes',value=100000000)
    st.write(wb.format_bytes(bytes))

#-----------------------------------------------------------------------
# format_milliseconds(milliseconds: int)
#-----------------------------------------------------------------------
st.markdown("""
#### :blue[format_milliseconds(milliseconds)]
Formats milliseconds into a human-readable string.

**Args:**  
milliseconds (int): The amount of milliseconds to format.

**Returns:**  
str: The formatted string of hours, minutes, seconds, milliseconds.
""")

with st.echo():
    st.write(wb.format_milliseconds(milliseconds))

#-----------------------------------------------------------------------
# run_query(query)
#-----------------------------------------------------------------------
st.markdown("""
#### :blue[run_query(query)]
Executes the given SQL query and returns the result as a pandas dataframe. 
In most use cases the <a href="#query-query-file-name-query-parameters">query(query_file_name, query_parameters='')</a> fucntion should be used to run SQL,
since it run sql stored in a file outside of the python app and can be parameterized.


**Parameters:**  
query (str): The SQL query to be executed.  
conn: A database connection object, assumed to be defined in the global scope.  

**Returns:**  
pandas.DataFrame: A dataframe containing the result of the query.  

**Example:** 
""",unsafe_allow_html=True)

with st.echo():
    df = wb.run_query('show warehouses')
    st.dataframe(df)

#-----------------------------------------------------------------------
# query(query_file_name, query_parameters='')
#-----------------------------------------------------------------------
st.markdown("""
#### :blue[query(query_file_name, query_parameters='')]
Connections to Snowflake and runs a SQL query by reading the query from a file and formatting it with the given parameters then returns the results as a dataframe.  Passed variable can be 
anywhere in the query (schema name, table name, predicate value, column name, etc). 

**Parameters:**  
*query_file_name (str)*: The name of the file containing the SQL query. The file should be located in the "queries" directory.  
*query_parameters (str, optional)*: A string containing the parameters to be formatted into the query. Default is an empty string. 
   
**Returns:**  
df: A pandas DataFrame containing the results of the query. If the query file is not found, an empty DataFrame is returned and an error message is printed.
   
**Example:**   
Thes following example run a query named template.sql, which must be in the sql/ folder, and passes dictionary of variables to be used in the query.   
The key, e.g. account, maps to a variable, e.g. account_selected.   The key is used in your sql files as {account} the varialbe is set in your python script. 
""")

st.code("""
df = wb.snowhouse_query('template.sql',
              {
               'query_id':query_id,
               'start_date':start_date,
               'end_date':end_date,
               'timezone':timezone
              }
            )
""")

st.markdown("""
The above function run the following template.sql file located in the sql/ folder.
""")
st.code("""
SELECT 
    current_timestamp() AS current_time
	,CURRENT_ACCOUNT() as account
	,CURRENT_REGION() as Region
	,CURRENT_ROLE() as role
	,CURRENT_WAREHOUSE() as warehouse
    ,'{query_id}' as query_id
    ,'{start_date}' as start_date
    ,'{end_date}' as end_date
    ,'{timezone}' as timezone
""")