import streamlit as st
from datetime import datetime, timedelta
from streamlit_date_picker import date_range_picker, PickerType
import snowflake.connector
import pandas as pd
import time

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
        schema='PUBLIC'
    )

# Fetch distinct names from Snowflake
@st.cache_data(show_spinner=False)
def fetch_distinct_names(_conn):
    cur = _conn.cursor()
    query = "SELECT DISTINCT NAME FROM STREAMLIT_APPS.PUBLIC.REP_LEAVE_PTO"
    cur.execute(query)
    names = [row[0] for row in cur.fetchall()]
    return names

# Fetch PTO data from Snowflake without caching
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

# Callback function to save changes with duplicate check only for new entries
def save_changes(edited_pto_df, selected_name, conn):
    cur = conn.cursor()

    # Update each entry in the PTO dataframe
    for index, row in edited_pto_df.iterrows():
        if pd.isnull(row['Date']):
            continue

        # Check and convert date string if necessary
        if isinstance(row['Date'], str):
            row['Date'] = pd.to_datetime(row['Date'])

        # Calculate hours worked
        hours_worked = 0.0 if row['PTO'] == 'Full Day' else 0.5

        # Update the Snowflake data
        update_query = f"""
        UPDATE STREAMLIT_APPS.PUBLIC.REP_LEAVE_PTO
        SET "Hours Worked Text" = %s, "Hours Worked" = %s
        WHERE NAME = %s AND "DATE" = %s
        """
        cur.execute(update_query, (row['PTO'], hours_worked, selected_name, row['Date']))
    
    conn.commit()
    cur.close()

    # Force refresh of data editor after saving
    st.session_state['pto_data'] = fetch_pto_data(conn, selected_name)

    # Show success message
    with st.sidebar:
        success_message = st.empty()
        success_message.success("Changes saved successfully!")
        time.sleep(3)
        success_message.empty()

# Function to reset session state when a new rep is selected
def reset_session_state_on_rep_change(selected_name):
    if 'last_selected_rep' not in st.session_state or st.session_state['last_selected_rep'] != selected_name:
        st.session_state['last_selected_rep'] = selected_name
        st.session_state['pto_data'] = None
        st.session_state['edited_pto_df'] = None

# Establish connection to Snowflake
conn = get_snowflake_connection()
names = fetch_distinct_names(conn)
names.insert(0, '')  # Add a placeholder

col1, spacer, col2 = st.columns([8, 0.1, 1])

with col1:
    selected_name = st.selectbox(
        '', 
        names, 
        key='select_rep', 
        format_func=lambda x: 'Select Sales Rep' if x == '' else x
    )

    reset_session_state_on_rep_change(selected_name)

    if selected_name != '':
        day_type = st.radio('', ['Full Day', 'Half Day'], key='day_type')
        default_start, default_end = datetime.now() - timedelta(days=1), datetime.now()

        date_range_string = date_range_picker(
            picker_type=PickerType.date,
            start=default_start, end=default_end,
            key='date_range_picker'
        )

        if date_range_string:
            start_date, end_date = date_range_string
            start_date = datetime.strptime(start_date, '%Y-%m-%d') if isinstance(start_date, str) else start_date
            end_date = datetime.strptime(end_date, '%Y-%m-%d') if isinstance(end_date, str) else end_date

            st.write(f"{start_date.strftime('%b %d, %Y')} - {end_date.strftime('%b %d, %Y')}")

            if st.button('Submit', key='submit_button'):
                cur = conn.cursor()
                current_date = start_date
                while current_date <= end_date:
                    if current_date.weekday() < 5:  # Ignore weekends
                        hours_worked_text = "Full Day" if day_type == 'Full Day' else "Half Day"
                        hours_worked = 0 if day_type == 'Full Day' else 0.5
                        insert_query = f"""
                        INSERT INTO STREAMLIT_APPS.PUBLIC.REP_LEAVE_PTO (NAME, "Hours Worked Text", "Hours Worked", "DATE")
                        VALUES (%s, %s, %s, %s)
                        """
                        cur.execute(insert_query, (selected_name, hours_worked_text, hours_worked, current_date))
                    current_date += timedelta(days=1)
                conn.commit()

                # Refresh data editor with updated data after submitting new PTO entries
                st.session_state['pto_data'] = fetch_pto_data(conn, selected_name)

                # Show success message
                with st.sidebar:
                    success_message = st.empty()
                    success_message.success(f"Time off submitted for {selected_name}.")
                    time.sleep(3)
                    success_message.empty()

    if selected_name != '':
        if 'pto_data' not in st.session_state or st.session_state['pto_data'] is None:
            st.session_state['pto_data'] = fetch_pto_data(conn, selected_name)

        pto_data = st.session_state['pto_data']
        original_pto_df = pd.DataFrame(pto_data, columns=["Date", "PTO"])
        original_pto_df['Date'] = pd.to_datetime(original_pto_df['Date']).dt.date

        if 'edited_pto_df' not in st.session_state:
            st.session_state['edited_pto_df'] = original_pto_df.copy()

        with st.sidebar:
            st.write("Edit PTO Entries:")
            st.session_state['edited_pto_df'] = st.data_editor(
                st.session_state['edited_pto_df'],
                num_rows="dynamic",
                column_config={
                    "Date": st.column_config.Column(label="Date", width=160),
                    "PTO": st.column_config.SelectboxColumn(
                        label="PTO", options=["Full Day", "Half Day"], width=110, required=True
                    ),
                },
                hide_index=True,
                key='data_editor_sidebar'
            )

        if st.sidebar.button("Save Changes", key='save_changes_button'):
            save_changes(st.session_state['edited_pto_df'], selected_name, conn)

            # Refresh data editor after saving changes
            st.session_state['pto_data'] = fetch_pto_data(conn, selected_name)
