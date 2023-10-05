# WhiteBear
The WhiteBear framework offers a streamlined solution for the development of interactive, multi-page data applications using Streamlit and Snowflake. 
It provides a set of pre-built functions, configuration files, and sample scripts that simplify the process of connecting to Snowflake, 
executing SQL queries, visualizing results, and incorporating user inputs.   SQL queries can be easily edited and reused as they are separated from the python code. This enables faster and more efficient development processes.



## Setup
1. Create a conda enviroment with the required dependincies.   
```bash
conda env create -f environment.yml
```
2. Active the environment
```bash
conda activate whitebear
```
3. Start the streamlit app
```bash
streamlit run Home.py
```

SEE INCLUDED pages/1000_Documenation.py FOR DETAILED AND INTERACTIVE DOCUMENTION

## Directory Structure
The directory structure organizes the codebase, data and config files into different sections.

| Directory   | Description                                                                                                                                                                                                                                                           |
| ----------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| - root -    | Home.py                                                                                                                                                                                                                                                               |
| .streamlit/ | Streamlit directory which contains the config.toml and secrets.toml files. The config.toml is optional (see [What is the path of Streamlit’s config.toml file? - Streamlit Docs](https://docs.streamlit.io/knowledge-base/using-streamlit/path-streamlit-config-toml)) the secrets.toml file is required.                                                                                                                                                                                                     |
| common/     | Contains the WhiteBear module, WhiteBear.py, which is a collection of python functions to be used your scripts.  Additional modules can be placed is this directory.                                                                                                   |
| data/       | Contains accounts.toml, which is used by the WhiteBear module.  Additionally, you can places any data files that maybe used by your scripts.                                                                                                                         |  
| pages/      | Contains streamlit scripts you create. Provided sripts include: (Documentation.py) a interactive documentation of WhiteBear functions, (test_connection.py) for testing connection to Snowflake and (template.py) that provdes a template for creating additional scripts. |
| resources/  | Contains any static resources such as images                                                                                                                                                                                                                                 |
| sql/        | Contains sql files that are used by (Steamlit) python apps you develope.                                                                                                     |

## Config Files
Streamlit and WhiteBear uses TOML (Tom's Obvious, Minimal Language) file format for configuration files.
It is designed to be easy to read and write, and aims to provide a more human-friendly alternative to other configuration file formats such as JSON and XML. 
TOML files contain key-value pairs separated by an equal sign (=), and the structure of the data is defined using square brackets ([]). 
The values can be strings, integers, floating-point numbers, booleans, dates, and arrays of these types. 

### accounts.toml
The *data/accounts.toml* file is utilized to store information regarding one or multiple Snowflake accounts. 
The values contained in this file serve as variables that are utilized in queries and Python scripts.  

Modify the section names and values to align with the Snowflake account values you will access. 
The section name, such as [PROD-ACCOUNT], will appear (sorted) as options in the account select box in your Streamlit apps. 
Hence, it is essential to choose a meaningful name that makes sense to you.
Section names may contain alphanumeric characters, underscores, and hyphens. 
However, they must not begin with a numeric character or contain spaces.
At least one account selection is mandatory, but you can add as many as you need.


 To access these values, the wb.account_sector() function is used. Upon selection of an account, a dictionary that contains all 
 values associated with the chosen account will be returned. If necessary, additional values can be added to this dictionary as required.

| **Value**    | **Type** | **Required** | **Description** |
| ------------ | ------ | -------------- | ------------------------------------------------------------ |
| description  | string |                |Description of account |
| environment  | string |                |Environment (Dev, QA, Prod, etc) |
| credit_cost  | float  | Yes            |Credit credit cost |
| storage_cost | float  | Yes            |Storage credit cost |
| timezone     | string | Yes            |Timezone for converting timestamp to the specified timezone.  e.g. America/Los_Angeles, America/New_York, Europe/Paris, etc. |
| connection   | string | Yes            |The connection name must match a key in the ./streamlit/secrets.toml file. |  


**Example of the data/accounts.toml file:** 

```
[PROD-ACCOUNT]
description  = "US Production"
environment  = "PROD"
credit_cost  = 2.00
storage_cost = 23.00
timezone     = "America/New_York"
connection   = "devaccount"

[DEV-ACCOUNT]
description  = "EU Developement"
environment  = "DEV"
credit_cost  = 3.00
storage_cost = 23.00
timezone     = "Europe/Paris"
connection   = "devaccount"
```

### secrets.toml

The .streamlit/secrets.toml file is a configuration file used by the Streamlit.  It contains the databases credentials needed to access Snowflake. 
The secrets in the file are stored as key-value pairs, with the keys being the names of the secrets and the values being the actual secrets themselves. 

For more information see: [Connect Streamlit to Snowflake - Streamlit Docs](https://docs.streamlit.io/knowledge-base/tutorials/databases/snowflake)

**Example:**   


```
[devaccount]
user          = "<user name>"
account       = "<snowflake account name>"
authenticator = "externalbrowser"
warehouse     = "mywarehouse"
role          = "technical_account_manager"

[prodaccount]
user      = "<user name>"
password  = "<password>"
account   = "<snowflake account name>"
warehouse = "<warehouse name>"
```

## Queries

Queries are stored as separate text files in the sql/ directory. Queries variables are variables names surrounded by curly brackets, e.g. {start_date}.
Variables can be used anywhere in the query (schema name, table name, predicate value, column name, etc).  

Variables can be named anything, but to make apps and queries more reusable it is best to use a common set of variable names.  




In the example below the {deployment} variables is used for the schema name and {account}, {start_date} and {end_date} variables are used in predicates. 

```sql
select error_code,
    any_value(error_message) AS error_message,
    count(*) AS query_count,
    sum(total_elapsed_time) AS total_elapsed_time
FROM snowflake.account_usage.query_history q
WHERE start_time BETWEEN '{start_date}' AND '{end_date}'
  AND error_code IS NOT NULL
GROUP BY error_code
ORDER BY total_elapsed_time desc
```

## Sample App

A Steamlit (python) app sets the value for the variables then call the WhiteBear wb.query() function with the sql file name and a dictionary that maps the variable in the sql file to the variables in the python app.   

Below is a full Streamlit app that uses the WhiteBear module and the SQL file above.   This app does the following:  
- Imports the WhiteBear module
- Places a dropdown lists, in the sidebar,  for the the users to select an account and date range.
- Calls the wb.query function with the query file name to run and the variables.  The query functions connects to the database, runs the query and returns the results as a dataframe.
- Display the dataframe created.  

```python
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
```


## Functions

SEE INCLUDED pages/1000_Documenation.py FOR DETAILED AND INTERACTIVE DOCUMENTION
