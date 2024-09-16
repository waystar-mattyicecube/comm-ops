import streamlit as st
from datetime import datetime, timedelta
from streamlit_date_picker import date_range_picker, PickerType
import snowflake.connector
import pandas as pd

# Logo URL
logo_url = "https://companieslogo.com/img/orig/WAY-3301bb15.png?t=1717743657"

# Display the logo and title inline using HTML and CSS with a smaller logo
st.markdown(
    f"""
    <div style="display: flex; align-items: center;">
        <img src="{logo_url}" alt="Company Logo" style="width:30px; margin-right:10px;">
        <h1 style="display: inline;">Sales PTO Tracking</h1>
    </div>
    """,
    unsafe_allow_html=True
)

# Inject custom CSS for the radio button and submit button styling
st.markdown(
    """
    <style>
    div[role="radiogroup"] > label > div:first-of-type {
        background-color: #0056b3 !important;
    }
    div[role="radiogroup"] > label:hover > div:first-of-type {
        background-color: #0056b3 !important;
    }
    div[role="radiogroup"] > label > div:first-of-type > div[aria-checked="true"] {
        border: 2px solid #0056b3 !important;
    }
    .stButton > button {
        background-color: #0056b3;
        color: white;
        border: none;
        padding: 0.5rem 1rem;
        font-size: 1rem;
        border-radius: 0.25rem;
        transition: background-color 0.3s ease, color 0.3s ease, border 0.3s ease;
    }
    .stButton > button:hover {
        background-color: white;
        color: #0056b3;
        border: 2px solid #0056b3;
    }
    .stButton > button:active {
        background-color: #0056b3 !important;
        color: white !important;
        border: none !important;
    }
    .stButton > button:focus {
        background-color: #0056b3 !important;
        color: white !important;
        border: none !important;
        outline: none !important;
    }
    </style>
    """,
    unsafe_allow_html=True
)

# Cache the Snowflake connection
@st.cache_resource
def get_snowflake_connection():
    return snowflake.connector.connect(
        user='mattyicecube',
        password='Mattman1159!',
        account='fna44578.east-us-2.azure',
        warehouse='COMPUTE_WH',
        database='STREAMLIT_APPS',
        schema='PUBLIC',
        client_session_keep_alive=True
    )

# Fetch distinct names from Snowflake
@st.cache_data(show_spinner=False)
def fetch_distinct_names(_conn):
    cur = _conn.cursor()
    query = "SELECT DISTINCT NAME FROM STREAMLIT_APPS.PUBLIC.REP_LEAVE_PTO"
    cur.execute(query)
    names = [row[0] for row in cur.fetchall()]
    return names

# Fetch PTO data from Snowflake
@st.cache_data(show_spinner=False)
def fetch_pto_data(_conn, selected_name):
    cur = _conn.cursor()
    query = f"""
    SELECT "DATE", "Hours Worked Text" 
    FROM STREAMLIT_APPS.PUBLIC.REP_LEAVE_PTO
    WHERE TRIM(NAME) = %s
    ORDER BY "DATE" DESC
    """
    cur.execute(query, (selected_name,))
    return cur.fetchall()

# Function to filter PTO data based on 'Recent' or 'All' selection
def filter_pto_data(pto_data, filter_type):
    pto_df = pd.DataFrame(pto_data, columns=["Date", "PTO"])
    pto_df['Date'] = pd.to_datetime(pto_df['Date'])

    today = pd.Timestamp.now().normalize()
    one_year_ago = today - pd.DateOffset(years=1)
    one_year_from_now = today + pd.DateOffset(years=1)

    if filter_type == 'Recent':
        filtered_df = pto_df[(pto_df['Date'] >= one_year_ago) & (pto_df['Date'] <= one_year_from_now)]
    else:
        filtered_df = pto_df

    return filtered_df

# Function to detect changes for insert/update
def get_changed_rows(edited_pto_df, original_pto_df):
    changed_rows = []

    for idx, edited_row in edited_pto_df.iterrows():
        if idx in original_pto_df.index:
            original_row = original_pto_df.loc[idx]
            if edited_row["Date"] != original_row["Date"] or edited_row["PTO"] != original_row["PTO"]:
                changed_rows.append(edited_row)
        else:
            changed_rows.append(edited_row)

    return pd.DataFrame(changed_rows)

# Function to check if any dates fall on a weekend
def check_for_weekend_dates(edited_pto_df):
    for date in edited_pto_df['Date']:
        if date.weekday() >= 5:  # 5 is Saturday, 6 is Sunday
            return True
    return False

# Function to insert, update, and delete records based on the data editor's changes
def save_data_editor_changes(edited_pto_df, original_pto_df, selected_name, conn):
    cur = conn.cursor()

    # Detect changed rows
    changed_rows_df = get_changed_rows(edited_pto_df, original_pto_df)

    # Check for weekend dates
    if check_for_weekend_dates(edited_pto_df):
        error_message = st.sidebar.error("PTO cannot occur on weekends. Please revise your entry.")
        st.experimental_rerun()  # Refresh page after displaying error message
        return False  # Prevent saving changes if weekend dates are found

    if not changed_rows_df.empty:
        # Check for duplicate PTO dates in Snowflake
        changed_dates = changed_rows_df['Date'].tolist()
        check_existing_dates_query = """
        SELECT "DATE" FROM STREAMLIT_APPS.PUBLIC.REP_LEAVE_PTO
        WHERE NAME = %s AND "DATE" IN ({})
        """.format(','.join(['%s'] * len(changed_dates)))

        cur.execute(check_existing_dates_query, [selected_name] + changed_dates)
        duplicate_dates = [row[0] for row in cur.fetchall()]

        if duplicate_dates:
            duplicate_dates_str = ', '.join([date.strftime('%b %d, %Y') for date in duplicate_dates])
            st.sidebar.error(f"PTO already exists for {selected_name} on: {duplicate_dates_str}.")
            return False  # Prevent further saving if duplicates exist

    # Detect deleted rows
    deleted_rows_df = original_pto_df.loc[~original_pto_df.index.isin(edited_pto_df.index)]

    # Delete rows from Snowflake
    for _, deleted_row in deleted_rows_df.iterrows():
        delete_query = """
        DELETE FROM STREAMLIT_APPS.PUBLIC.REP_LEAVE_PTO
        WHERE NAME = %s AND "DATE" = %s
        """
        cur.execute(delete_query, (selected_name, deleted_row["Date"]))

    # Insert or update rows in Snowflake
    for idx, edited_row in edited_pto_df.iterrows():
        original_row = original_pto_df.loc[idx] if idx in original_pto_df.index else None

        hours_worked = 0.5 if edited_row["PTO"] == 'Half Day' else 0.0  # Set Hours Worked

        if original_row is not None:
            if edited_row["Date"] != original_row["Date"] or edited_row["PTO"] != original_row["PTO"]:
                update_query = """
                UPDATE STREAMLIT_APPS.PUBLIC.REP_LEAVE_PTO
                SET "DATE" = %s, "Hours Worked Text" = %s, "Hours Worked" = %s
                WHERE NAME = %s AND "DATE" = %s
                """
                cur.execute(update_query, (edited_row["Date"], edited_row["PTO"], hours_worked, selected_name, original_row["Date"]))
        else:
            insert_query = """
            INSERT INTO STREAMLIT_APPS.PUBLIC.REP_LEAVE_PTO (NAME, "DATE", "Hours Worked Text", "Hours Worked")
            VALUES (%s, %s, %s, %s)
            """
            cur.execute(insert_query, (selected_name, edited_row["Date"], edited_row["PTO"], hours_worked))

    conn.commit()
    cur.close()

    # Refresh PTO data after saving changes
    updated_pto_data = fetch_pto_data(conn, selected_name)
    st.session_state['pto_data'] = updated_pto_data

    return True

# Callback for saving changes in data editor
def on_save_changes(selected_name, edited_pto_df, original_pto_df, conn):
    if save_data_editor_changes(edited_pto_df, original_pto_df, selected_name, conn):
        st.sidebar.success("Changes saved successfully.")
    else:
        st.sidebar.error("Failed to save changes.")

# Handle session state for sales rep
def reset_session_state_on_rep_change(selected_name):
    if 'last_selected_rep' in st.session_state and st.session_state['last_selected_rep'] != selected_name:
        st.session_state['pto_data'] = None
        st.session_state['last_selected_rep'] = selected_name

# Main app logic
conn = get_snowflake_connection()

# Sidebar filters
st.sidebar.title('Filters')

# Radio buttons for 'Recent' or 'All'
filter_type = st.sidebar.radio('Select Filter', ('Recent', 'All'), index=0)

# Fetch distinct names for the sales rep dropdown
names = fetch_distinct_names(conn)

# Sales rep dropdown
selected_name = st.sidebar.selectbox('Select Sales Rep', names)

# Reset session state if sales rep changes
reset_session_state_on_rep_change(selected_name)

# Display existing PTO data
if 'pto_data' not in st.session_state or st.session_state['pto_data'] is None:
    st.session_state['pto_data'] = fetch_pto_data(conn, selected_name)

pto_data = st.session_state['pto_data']

# Filter PTO data based on selection
filtered_pto_data = filter_pto_data(pto_data, filter_type)
filtered_pto_df = pd.DataFrame(filtered_pto_data, columns=["Date", "PTO"])

# Display data editor
with st.sidebar:
    st.header("Data Editor")
    
    # Data editor for filtered PTO data
    edited_pto_df = st.data_editor(
        filtered_pto_df,
        use_container_width=True
    )

    # Save changes button
    if st.button("Save Changes"):
        on_save_changes(selected_name, edited_pto_df, filtered_pto_df, conn)

# Display error message for weekend dates
if 'error_message' in st.session_state and st.session_state['error_message']:
    st.write(st.session_state['error_message'])
    st.session_state['error_message'] = None  # Clear message after displaying

# Display PTO data
st.write("### PTO Data")
st.dataframe(filtered_pto_df, use_container_width=True)

# Handle date range picker and success/error messages
with st.container():
    st.write("### Date Range Picker")
    start_date, end_date = date_range_picker(
        "Select Date Range",
        default_start_date=datetime.now() - timedelta(days=30),
        default_end_date=datetime.now(),
        picker_type=PickerType.RANGE
    )
    
    if start_date and end_date:
        st.write(f"Selected Date Range: {start_date} to {end_date}")

    # Display success message for date picker
    if 'date_picker_message' in st.session_state:
        st.success(st.session_state['date_picker_message'])
        st.session_state['date_picker_message'] = None

# Close the Snowflake connection
conn.close()
