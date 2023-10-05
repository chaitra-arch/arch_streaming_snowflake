"""
File: whitebear.py
Description: This module provides functionality to perform various data processing tasks.
Author: michael.henson@snowflake.com
Change Log:
- 2023-03-21: Moved init_connection() to functions
- 2023-03-20: Removed deprecated function get_column_list()
"""
import streamlit as st
import snowflake.connector
import pandas as pd
import os
import re
import datetime
import math
import sqlparse 
import tomli 
from dateutil.relativedelta import relativedelta
from streamlit_extras.app_logo import add_logo
from streamlit_extras.no_default_selectbox import selectbox
from streamlit_extras.metric_cards import style_metric_cards
from streamlit_extras.mandatory_date_range import date_range_picker


with open("data/accounts.toml", mode="rb") as fp:
     accounts = tomli.load(fp)

def page_setup(title):
    """Set up the page layout with a logo and a header.

    Parameters:
        title (str): The title of the page.

    Returns:
        None
    """
    add_logo("resources/logo.png", height=50)
    st.header(title)
    return

# Snowflake Connection 
@st.cache_resource
def init_connection(connection):
    return snowflake.connector.connect(**st.secrets[connection], client_session_keep_alive=True    )

# conn = init_connection(st.session_state.connection)

@st.cache_data(ttl=st.session_state['ttl'])
def cache_query(query) -> pd.DataFrame:
    """Executes the given SQL query and returns the result as a pandas dataframe.

    Parameters:
    query (str): The SQL query to be executed.
    conn: A database connection object, assumed to be defined in the global scope.

    Returns:
    pandas.DataFrame: A dataframe containing the result of the query.
    """
    conn = init_connection(st.session_state.connection)
    
    cur = conn.cursor()
    cur.execute(query)
    df = pd.DataFrame.from_records(cur.fetchall(), columns=[desc[0] for desc in cur.description])

    # result = cur.fetchall()
    # df = pd.DataFrame(result, columns=[desc[0] for desc in cur.description])
    return df

def run_query(query) -> pd.DataFrame:
    """Executes the given SQL query and returns the result as a pandas dataframe.

    Parameters:
    query (str): The SQL query to be executed.
    conn: A database connection object, assumed to be defined in the global scope.

    Returns:
    pandas.DataFrame: A dataframe containing the result of the query.
    """
    conn = init_connection(st.session_state.connection)

    cur = conn.cursor()
    cur.execute(query)
    df = pd.DataFrame.from_records(cur.fetchall(), columns=[desc[0] for desc in cur.description])

    return df

def camelcase(s):
    """
    Convert a string to camelcase.

    Replaces any dashes or underscores in the string with spaces,
    capitalizes the first letter of each word, and removes the spaces
    between words.

    Parameters:
        s (str): The string to convert to camelcase.

    Returns:
        str: The camelcased string.

    Example:
        >>> camelcase("hello-world")
        'HelloWorld'
        >>> camelcase("my_name_is_jeff")
        'My Name Is Jeff'
    """
    # Replace any - or _ with a space
    s = re.sub(r"(_|-)+", " ", s)
    # Capitalize the first letter of each word and convert the rest to lowercase
    s = s.title()
    # Remove spaces between words
    #s = s.replace(" ", "")
    return s

def query(query_file_name, query_parameters='', cache=True) -> pd.DataFrame:
    """
    Run a SQL query by reading the query from a file and formatting it with the given parameters.
    
    Parameters:
    - query_file_name (str): The name of the file containing the SQL query. The file should be located in the "queries" directory.
    - query_parameters (str, optional): A string containing the parameters to be formatted into the query. Default is an empty string.
    
    Returns:
    - df: A pandas DataFrame containing the results of the query. If the query file is not found, an empty DataFrame is returned and an error message is printed.
    """
    filename = 'sql/'+query_file_name
    if os.path.isfile(filename):
        with open(filename) as f:
            raw_query = f.read()
        
        query = raw_query.format_map(query_parameters)
        debug(format_sql(format_sql(query)), 'Source SQL: ' + filename)
        if cache: 
            df = cache_query(query)
        else:
            df = run_query(query)
    else:
        # File passed not found, create an empty dataframe and write error to app and terminal 
        df = pd.DataFrame()
        st.error(f"**ERROR-** SQL file '{filename}' not found")
        print(f"ERROR: SQL file '{filename}' not found")    
    return df    

def account_selector() ->dict:
    """
    Provides a streamlit selectbox to choose an account from the accounts.toml files.

    Returns:
    -------
    account : dict
        The selected account. If no account is selected, returns None.
    """
    account_list = list(accounts.keys())
    account_list.sort()
    account_selected = selectbox('Account',account_list)
    account = None if account_selected == None else accounts[account_selected]
    return account

def account_db() -> str:
    db_name = st.text_input('DB Name', '')
    return db_name

def account_schema() -> str:
    sc_name = st.text_input('Schema Name', '')
    return sc_name

def account_network_policy_filter() -> str:
    npf_name = st.text_input('Network Policy Filter', '10.')
    return npf_name

def query_id_selector(account_name, label='Saved Query ID'):
    """Retrieves a saved query ID for the given account name.

    Parameters:
        account_name (str): The name of the account to retrieve the query ID for.

    Returns:
        str or None: The query ID if it is found, or None if it is not found or the file does not exist.

    Raises:
        None.

    Reads a CSV file called 'data/query_uuid.csv' and filters it based on the given account name. 
    If there is a saved query ID for the account, it will display a dropdown list of the saved query labels 
    (stored in the 'label' column of the CSV file) using Streamlit's `selectbox()` function. If a label is selected, 
    the corresponding query ID (stored in the 'QUERY_ID' column of the CSV file) is returned. If no label is selected, 
    or if the file does not exist or the account name is not found, None is returned.
    """
    file_name = 'data/query_uuid.csv'
    if os.path.exists(file_name):
        df = pd.read_csv(file_name) 
        df = df[df['ACCOUNT_NAME'] == account_name]
        label_list = df['LABEL'].tolist()
        label_list.sort()
        label = selectbox(label,label_list)
        if label:
            query_id = df.loc[df['LABEL'] == label, 'QUERY_ID'].values[0]
        else: 
            query_id = None 
    else:
        query_id = None
    return query_id

def debug(text, label ="Debug"):
    """
    Debugging helper function that displays code or text in the Streamlit app if the `debug` flag is set to True.

    :param text: The code or text to be displayed.
    :type text: str
    :param label: The label for the collapsible section that displays the code or text.
    :type label: str
    :return: None
    :rtype: None
    """
    if st.session_state.debug:
        with st.expander(label):
            st.code(text)
    return

def format_bytes(size_bytes: int) -> str:
    """
    Formats a size in bytes to a string in KB, MB, GB, etc.

    Parameters:
    size_bytes (int): The size in bytes.

    Returns:
    str: The formatted size in KB, MB, GB, etc.
    """
    basis = 1024 
    if size_bytes == 0:
        return "0 B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_bytes, basis)))
    p = math.pow(basis, i)
    s = round(size_bytes / p, 2)
    return "%s %s" % (s, size_name[i]) + " " * 8

def format_milliseconds(milliseconds: int, short: bool = False) -> str:
    """
    Formats a duration given in milliseconds into a human-readable string.

    Parameters:
        milliseconds (int): The duration to format, in milliseconds.
        short (bool, optional): If True, use a short format (e.g. "5 min" instead of "5 minutes").
            Defaults to False.

    Returns:
        str: A human-readable string representing the duration.

    Examples:
        >>> format_milliseconds(1500)
        '1 sec 500 ms'
        >>> format_milliseconds(1500, short=True)
        '1.50 seconds'
        >>> format_milliseconds(3600000)
        '1 hr'
    """
    ms = milliseconds
    seconds, milliseconds = divmod(milliseconds, 1000)
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)

    pretty_print = ""
    if short:
        if ms < 1000:
            pretty_print = f"{ms} milliseconds"
        elif ms < 60000:
            pretty_print =  f"{ms / 1000.0:.2f} seconds"
        elif ms < 3600000:
            pretty_print =  f"{ms / (1000.0 * 60):.2f} minutes"
        else:
            pretty_print =  f"{ms / (1000.0 * 60 * 60):.2f} hours"
    else:
        if hours:
            pretty_print += f"{hours} hr{'s' if hours > 1 else ''} "
        if minutes:
            pretty_print += f"{minutes} min{'s' if minutes > 1 else ''} "
        if seconds:
            pretty_print += f"{seconds} sec{'s' if seconds > 1 else ''} "
        if milliseconds:
            pretty_print += f"{milliseconds} ms{'' if milliseconds > 1 else ''}"
    return pretty_print

def format_seconds(seconds: int) -> str:
    """
    Formats seconds into a human-readable string.

    Parameters:
    seconds (int): The amount of seconds to format.

    Returns:
    str: The formatted string of hours, minutes, seconds.
    """
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)

    pretty_print = ""
    if hours:
        pretty_print += f"{hours} hr{'s' if hours > 1 else ''} "
    if minutes:
        pretty_print += f"{minutes} min{'s' if minutes > 1 else ''} "
    if seconds:
        pretty_print += f"{seconds} sec{'s' if seconds > 1 else ''} "
    return pretty_print

def convert_milliseconds(milliseconds, to_unit='seconds') -> float:
    """
    Convert the given number of milliseconds to the specified unit.
    
    Parameters:
        milliseconds (int): The number of milliseconds to be converted.
        to_unit (str): The unit to convert the milliseconds to. Default is 'seconds'.
                       Accepted values are 'seconds', 'minutes', 'hours'.
                       
    Returns:
        float: The converted value in the specified unit.  -1 is returned if  
               a 'Invalid unit' is specified not one of the accepted values.
    """
    if to_unit == 'seconds':
        return milliseconds / 1000
    elif to_unit == 'minutes':
        return milliseconds / (1000 * 60)
    elif to_unit == 'hours':
        return milliseconds / (1000 * 60 * 60)
    else:
        return -1

def date_selector():
    """
    Provides a user interface to select a date range. It returns a tuple of two dates, the start date and the end date.
    The function provides options to select a predefined date range, or a custom range by selecting the start and end date.
    It also includes an option to exclude today's date from the range.
    """
    date_ranges = [
        'Today',
        'Yesterday',
        'Last 7 days',
        'Last week (Sat-Sun)',
        'Last week (Mon-Fri)', 
        'Last 30 days',
        'Last month',
        'Last 3 months',
        'Last 6 months',
        'Last 12 months',
        'All time',
        'Custom',
    ]

    if 'date_range' in st.session_state:
        index = date_ranges.index(st.session_state.date_range)
    else:
        index = 0

    date_range = st.selectbox('Date range',options=date_ranges,index=index,key='date_range',)
    
    # Exclude Today's Date  (defaults to False) 
    if date_range in ['Today','Yesterday','Last month']:    
        exclude_today = st.checkbox('Exclude today',value=False, disabled=True)
    else:
        exclude_today = st.checkbox('Exclude today',value=True)

    date_to = datetime.date.today() 

    if date_range != 'Custom':
        date_to = datetime.date.today() - datetime.timedelta(days=1) if exclude_today else datetime.date.today()
        if date_range == 'Last 7 days':
            date_from = date_to - datetime.timedelta(days=7)
        elif date_range == 'Last 30 days':
            date_from = date_to - datetime.timedelta(days=30)
        elif date_range == 'Last month':
            date_from = (datetime.date.today()  - relativedelta(months=1)).replace(day=1)
            date_to = (date_from + relativedelta(months=1) - datetime.timedelta(days=1))
        elif date_range == 'Last 3 months':
            date_from = date_to - datetime.timedelta(weeks=12)
        elif date_range == 'Last 6 months':
            date_from = date_to - datetime.timedelta(weeks=24)
        elif date_range == 'Last 12 months':
            date_from = date_to - datetime.timedelta(days=365)
        elif date_range =='Today':
            date_from = datetime.date.today()
            date_to   = datetime.date.today() + datetime.timedelta(days=1) 
        elif date_range =='Yesterday':    
            date_from = datetime.date.today() - datetime.timedelta(days=1)
            date_to = datetime.date.today() - datetime.timedelta(days=1)
        elif date_range =='Last week (Sat-Sun)':    
            date_to = datetime.date.today() - datetime.timedelta(7+((datetime.date.today().weekday() + 1) % 7)-6 )
            date_from = date_to - datetime.timedelta(days=6)
        elif date_range =='Last week (Mon-Fri)':    
            date_to = datetime.date.today() - datetime.timedelta(7+((datetime.date.today().weekday() + 1) % 7)-5 )
            date_from = date_to - datetime.timedelta(days=6)
        else:
            date_from = datetime.date(year=2016, month=1, day=1)

    if 'custom' in st.session_state:
        value = st.session_state.custom
    else:
        value = (
            date_to - datetime.timedelta(days=7),
            date_to,
        )

    if date_range == 'Custom':
        date_from, date_to = st.date_input(
            'Choose start and end date',
            value=value,
            key='custom',
        )

    st.caption(f'Date range is from **{date_from}** to **{date_to}**')

    return date_from, date_to


def format_sql(query: str) -> str:
    """
    Formats the given SQL query using sqlparse library
    :param query: str, SQL query to be formatted
    :return: str, formatted SQL query
    """
    return sqlparse.format(
        query,
        reindent=True,
        keyword_case="upper",
        comma_first=False,
        identifier_case="lower",
        use_space_around_operators=True,
    )

def list_2_comma_str(input_list) -> str:
    """Convert a python list to a comma delitimed string for use in SQL IN statement"""
    output_string = ''
    for index, member in enumerate(input_list):
        if member[0] != '-':
            index +=1
            if index == 1:
                output_string += "'" + member + "'"
            else:
                output_string += ", '" + member + "'"
    return output_string

def column_selectbox(label: str, df: pd.DataFrame, column: str) -> str:
    """
    Creates a select box widget containing unique values from a column in a Pandas DataFrame.

    Parameters:
        label (str): The label to display for the select box widget.
        df (pd.DataFrame): The DataFrame containing the column of interest.
        column (str): The name of the column to extract unique values from.

    Returns:
        str: The selected value from the select box.

    Example:
        >>> column_selectbox("Select a fruit:", df, "fruit_type")
        'apple'
    """
    list_output = list(df[column].drop_duplicates())
    list_output.sort()
    return selectbox(label, list_output)  


def calculate_age(date: datetime.date, units: str = "years") -> int:
    """
    Calculates the age in specified units between today and the provided date.

    Parameters:
    date (datetime.date): The date to calculate the age from.
    units (str, optional): The units to calculate the age in. Options are "years", "months", or "days". Default is "years".

    Returns:
    int: The calculated age.

    Raises:
    ValueError: If the provided units are not one of the accepted options.

    Example:
    >>> calculate_age(datetime.date(2000, 1, 1))
    23
    >>> calculate_age(datetime.date(2000, 1, 1), "months")
    276
    >>> calculate_age(datetime.date(2000, 1, 1), "days")
    8475

    """
    today = datetime.date.today()
    age_in_days = (today - date).days
    if units == "years":
        age = age_in_days // 365
    elif units == "months":
        age = age_in_days // 30
    elif units == "days":
        age = age_in_days
    else:
        raise ValueError("Invalid units. Choose 'years', 'months', or 'days'.")
    return age


def format_age(date: datetime.date) -> str:
    """
    Calculates the difference between today's date and the input date and returns a string representation
    of the difference in years, months, and days.

    Parameters:
    date (datetime.date): The input date to compare against today's date.

    Returns:
    str: A string representation of the difference in years, months, and days, in the format:
        "X year(s) Y month(s) Z day(s)"
    """
    today = datetime.date.today()
    age_in_days = (today - date).days if today > date else (date - today).days
    years, months, days = age_in_days // 365, (age_in_days % 365) // 30, (age_in_days % 365) % 30

    pretty_print = ""
    if years:
        pretty_print += f"{years} year{'s' if years > 1 else ''} "
    if months:
        pretty_print += f"{months} month{'s' if months > 1 else ''} "
    if days:
        pretty_print += f"{days} day{'s' if days > 1 else ''} "

    return pretty_print

def determine_unit(milliseconds):
    """
    Determines the appropriate time unit for a given number of milliseconds.

    Parameters:
    milliseconds (int): The number of milliseconds.

    Returns:
    str: A string containing the converted value and the appropriate unit (either 'milliseconds', 'seconds', 'minutes', or 'hours').
    """

    if milliseconds < 1000:
        return f"{milliseconds} milliseconds"
    elif milliseconds < 60000:
        return f"{milliseconds / 1000.0:.2f} seconds"
    elif milliseconds < 3600000:
        return f"{milliseconds / (1000.0 * 60):.2f} minutes"
    else:
        return f"{milliseconds / (1000.0 * 60 * 60):.2f} hours"


if __name__ == "__main__":
    pass
