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
def save_changes(edited_pto_df, original_pto_df, selected_name, conn):
    cur = conn.cursor()

    # Handle updates and insertions for remaining entries
    for index, row in edited_pto_df.iterrows():
        if pd.isnull(row['Date']):
            with st.sidebar:
                warning_message = st.empty()
                warning_message.warning(f"Skipping invalid date in row {index}")
                time.sleep(5)
                warning_message.empty()
            continue

        if isinstance(row['Date'], str):
            row['Date'] = pd.to_datetime(row['Date'])

        if row['Date'].weekday() in [5, 6]:  # Skip weekends
            continue

        hours_worked = 0.0 if row['PTO'] == 'Full Day' else 0.5

        update_query = f"""
        UPDATE STREAMLIT_APPS.PUBLIC.REP_LEAVE_PTO
        SET "Hours Worked Text" = %s, "Hours Worked" = %s, "DATE" = %s
        WHERE NAME = %s AND "DATE" = %s
        """
        cur.execute(update_query, (row['PTO'], hours_worked, row['Date'], selected_name, row['Date']))
    
    conn.commit()
    cur.close()

    # Fetch updated PTO data after changes are saved and refresh the editor
    new_pto_data = fetch_pto_data(conn, selected_name)
    st.session_state['pto_data'] = new_pto_data  # Update session state with refreshed data

    with st.sidebar:
        success_message = st.empty()
        success_message.success("Changes saved successfully!")
        time.sleep(3)
        success_message.empty()

# Establish connection to Snowflake and fetch distinct names
conn = get_snowflake_connection()
names = fetch_distinct_names(conn)
names.insert(0, '')  # Add a placeholder for the selectbox

col1, spacer, col2 = st.columns([8, 0.1, 1])

with col1:
    selected_name = st.selectbox(
        '', 
        names, 
        key='select_rep', 
        format_func=lambda x: 'Select Sales Rep' if x == '' else x
    )

    if selected_name != '':
        day_type = st.radio('', ['Full Day', 'Half Day'], key='day_type')
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

            if st.button('Submit', key='submit_button'):
                check_query = f"""
                SELECT "DATE" FROM STREAMLIT_APPS.PUBLIC.REP_LEAVE_PTO
                WHERE NAME = %s AND "DATE" BETWEEN %s AND %s
                """
                cur = conn.cursor()
                cur.execute(check_query, (selected_name, start_date, end_date))
                existing_dates = [row[0] for row in cur.fetchall()]

                if existing_dates:
                    existing_dates_str = ', '.join([date.strftime('%b %d, %Y') for date in existing_dates])
                    with st.sidebar:
                        error_message = st.empty()
                        error_message.error(f"PTO already exists for {selected_name} on: {existing_dates_str}.")
                    time.sleep(5)
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
                            cur.execute(insert_query, (selected_name, hours_worked_text, hours_worked, current_date))
                        current_date += timedelta(days=1)
                    conn.commit()

                    with st.sidebar:
                        success_message = st.empty()
                        success_message.success(f"Time off submitted for {selected_name} from {formatted_start_date} to {formatted_end_date} (excluding weekends).")
                    time.sleep(5)
                    success_message.empty()

                    # Fetch updated PTO data and update the editor
                    new_pto_data = fetch_pto_data(conn, selected_name)
                    st.session_state['pto_data'] = new_pto_data

    if selected_name != '':
        if 'pto_data' not in st.session_state:
            pto_data = fetch_pto_data(conn, selected_name)
            st.session_state['pto_data'] = pto_data
        else:
            pto_data = st.session_state['pto_data']

        edited_pto_df = pd.DataFrame(pto_data, columns=["Date", "PTO"])
        edited_pto_df['Date'] = pd.to_datetime(edited_pto_df['Date']).dt.date

        original_pto_df = edited_pto_df.copy()

        # Render the data editor in the sidebar (only once, and update it on changes)
        with st.sidebar:
            st.write("Edit PTO Entries:")
            edited_pto_df = st.data_editor(
                edited_pto_df,
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

        # Save changes button to save edits
        if st.sidebar.button(
            "Save Changes", 
            key='save_changes_button'
        ):
            save_changes(edited_pto_df, original_pto_df, selected_name, conn)
