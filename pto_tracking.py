import streamlit as st
from datetime import datetime, timedelta
from streamlit_date_picker import date_range_picker, PickerType
import snowflake.connector
import time
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

# Initialize session states
if 'snowflake_connected' not in st.session_state:
    st.session_state['snowflake_connected'] = False
if 'selected_name' not in st.session_state:
    st.session_state['selected_name'] = 'Select Sales Rep'
if 'pto_df' not in st.session_state:
    st.session_state['pto_df'] = pd.DataFrame()

# Snowflake connection details
snowflake_user = 'mattyicecube'
snowflake_password = 'Mattman1159!'
snowflake_account = 'fna44578.east-us-2.azure'
snowflake_warehouse = 'COMPUTE_WH'
snowflake_database = 'STREAMLIT_APPS'
snowflake_schema = 'PUBLIC'

# Establish connection to Snowflake if not already connected
if not st.session_state['snowflake_connected']:
    try:
        conn = snowflake.connector.connect(
            user=snowflake_user,
            password=snowflake_password,
            account=snowflake_account,
            warehouse=snowflake_warehouse,
            database=snowflake_database,
            schema=snowflake_schema
        )
        st.session_state['snowflake_connected'] = True

        connection_message = st.empty()
        connection_message.success("Connected to Snowflake successfully.")
        time.sleep(3)
        connection_message.empty()
    except Exception as e:
        st.error(f"Error connecting to Snowflake: {e}")
else:
    conn = snowflake.connector.connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_account,
        warehouse=snowflake_warehouse,
        database=snowflake_database,
        schema=snowflake_schema
    )

# Fetch distinct values for the NAME column in STREAMLIT_APPS.PUBLIC.REP_LEAVE_PTO
query = "SELECT DISTINCT NAME FROM STREAMLIT_APPS.PUBLIC.REP_LEAVE_PTO"
cur = conn.cursor()
cur.execute(query)
names = [row[0] for row in cur.fetchall()]

# Add default option for the dropdown
names.insert(0, 'Select Sales Rep')

# Layout with a wider first column, spacer, and a second column
col1, spacer, col2 = st.columns([8, 0.1, 1])

# In the first column, display the dropdown and inputs for PTO submission
with col1:
    st.session_state['selected_name'] = st.selectbox('', names, index=names.index(st.session_state['selected_name']))

    if st.session_state['selected_name'] != 'Select Sales Rep':
        day_type = st.radio('', ['Full Day', 'Half Day'])
        default_start, default_end = datetime.now() - timedelta(days=1), datetime.now()
        refresh_value = timedelta(days=1)

        date_range_string = date_range_picker(picker_type=PickerType.date,
                                              start=default_start, end=default_end,
                                              key='date_range_picker',
                                              refresh_button={'is_show': False, 'button_name': 'Refresh Last 1 Days',
                                                              'refresh_value': refresh_value})

        if date_range_string:
            start_date, end_date = date_range_string
            start_date = datetime.strptime(start_date, '%Y-%m-%d') if isinstance(start_date, str) else start_date
            end_date = datetime.strptime(end_date, '%Y-%m-%d') if isinstance(end_date, str) else end_date

            formatted_start_date = start_date.strftime('%b %d, %Y')
            formatted_end_date = end_date.strftime('%b %d, %Y')

            st.write(f"{formatted_start_date} - {formatted_end_date}")

            if st.button('Submit'):
                check_query = f"""
                SELECT "DATE" FROM STREAMLIT_APPS.PUBLIC.REP_LEAVE_PTO
                WHERE NAME = %s AND "DATE" BETWEEN %s AND %s
                """
                cur.execute(check_query, (st.session_state['selected_name'], start_date, end_date))
                existing_dates = [row[0] for row in cur.fetchall()]

                if existing_dates:
                    existing_dates_str = ', '.join([date.strftime('%b %d, %Y') for date in existing_dates])
                    error_message = st.empty()
                    error_message.error(f"PTO already exists for {st.session_state['selected_name']} on: {existing_dates_str}.")
                    time.sleep(10)
                    error_message.empty()
                else:
                    hours_worked_text = "Full Day" if day_type == 'Full Day' else "Half Day"
                    hours_worked = 0 if day_type == 'Full Day' else 0.5
                    current_date = start_date
                    while current_date <= end_date:
                        if current_date.weekday() < 5:  # Ignore weekends
                            insert_query = f"""
                            INSERT INTO STREAMLIT_APPS.PUBLIC.REP_LEAVE_PTO (NAME, "Hours Worked Text", "Hours Worked", "DATE")
                            VALUES (%s, %s, %s, %s)
                            """
                            cur.execute(insert_query, (st.session_state['selected_name'], hours_worked_text, hours_worked, current_date))
                        current_date += timedelta(days=1)
                    conn.commit()

                    success_message = st.empty()
                    success_message.success(f"Time off submitted for {st.session_state['selected_name']} from {formatted_start_date} to {formatted_end_date} (excluding weekends).")
                    time.sleep(3)
                    success_message.empty()

# Move PTO data display to the sidebar and allow editing
if st.session_state['selected_name'] != 'Select Sales Rep':
    # Fetch PTO data from Snowflake only once
    if st.session_state['pto_df'].empty:
        pto_query = f"""
        SELECT "DATE", "Hours Worked Text" FROM STREAMLIT_APPS.PUBLIC.REP_LEAVE_PTO
        WHERE NAME = %s
        ORDER BY "DATE" DESC
        """
        cur.execute(pto_query, (st.session_state['selected_name'],))
        pto_data = cur.fetchall()
        if pto_data:
            st.session_state['pto_df'] = pd.DataFrame(pto_data, columns=["Date", "PTO"])
            st.session_state['pto_df']['Date'] = pd.to_datetime(st.session_state['pto_df']['Date']).dt.date

    # Separate the edited dataframe from the session state
    edited_pto_df = st.data_editor(st.session_state['pto_df'].copy(), height=300)

    if st.button("Save Changes"):
        # Handle edits: Update session state with new edited dataframe
        st.session_state['pto_df'] = edited_pto_df

        # Detect deleted rows and perform batch delete
        deleted_rows = st.session_state['pto_df'][~st.session_state['pto_df']['Date'].isin(edited_pto_df['Date'])]

        # Handle deletes and updates here, similar to your original logic

    cur.close()
    conn.close()
