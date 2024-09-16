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
        border: none !important.
    }
    .stButton > button:focus {
        background-color: #0056b3 !important;
        color: white !important;
        border: none !important;
        outline: none !important.
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
    today = datetime.now().date()
    three_months_ago = today - timedelta(days=90)
    one_year_from_now = today + timedelta(days=365)

    if filter_type == 'Recent':
        filtered_data = [row for row in pto_data if (three_months_ago <= row[0] <= one_year_from_now)]
    else:
        filtered_data = pto_data

    return filtered_data

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
        st.sidebar.error("You cannot select a date that falls on a weekend (Sat, Sun).")
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
    success = save_data_editor_changes(edited_pto_df, original_pto_df, selected_name, conn)

    if success:
        st.success("Changes saved successfully!")

# Reset session state when the sales rep is selected
def reset_session_state_on_rep_change(selected_name):
    if 'last_selected_rep' not in st.session_state or st.session_state['last_selected_rep'] != selected_name:
        st.session_state['last_selected_rep'] = selected_name
        st.session_state['pto_data'] = None  # Reset to force refresh

# Establish connection to Snowflake and fetch names
conn = get_snowflake_connection()
names = fetch_distinct_names(conn)
names.insert(0, '')  # Placeholder for selectbox

col1, spacer, col2 = st.columns([8, 0.1, 1])

# Main screen error/success placeholders
main_error_message = st.empty()
main_success_message = st.empty()

with col1:
    selected_name = st.selectbox(
        'Select a Sales Rep', 
        names, 
        key='select_rep', 
        format_func=lambda x: 'Select Sales Rep' if x == '' else x
    )

    reset_session_state_on_rep_change(selected_name)

    if selected_name != '':
        day_type = st.radio('Day Type', ['Full Day', 'Half Day'], key='day_type')
        default_start, default_end = datetime.now() - timedelta(days=1), datetime.now()

        # Removed `refresh_value` to fix the KeyError issue
        date_range_string = date_range_picker(
            picker_type=PickerType.date,
            start=default_start,
            end=default_end,
            key='date_range_picker',
        )

        if date_range_string:
            start_date, end_date = date_range_string
            start_date = datetime.strptime(start_date, '%Y-%m-%d') if isinstance(start_date, str) else start_date
            end_date = datetime.strptime(end_date, '%Y-%m-%d') if isinstance(end_date, str) else end_date

            formatted_start_date = start_date.strftime('%b %d, %Y')
            formatted_end_date = end_date.strftime('%b %d, %Y')

            st.write(f"{formatted_start_date} - {formatted_end_date}")

            if st.button('Submit', key='submit_button'):
                cur = conn.cursor()
                check_query = f"""
                SELECT "DATE" FROM STREAMLIT_APPS.PUBLIC.REP_LEAVE_PTO
                WHERE NAME = %s AND "DATE" BETWEEN %s AND %s
                """
                cur.execute(check_query, (selected_name, start_date, end_date))
                existing_dates = [row[0] for row in cur.fetchall()]

                if existing_dates:
                    existing_dates_str = ', '.join([date.strftime('%b %d, %Y') for date in existing_dates])
                    main_error_message.error(f"PTO already exists for {selected_name} on: {existing_dates_str}.")
                else:
                    current_date = start_date
                    hours_worked_text = "Full Day" if day_type == 'Full Day' else "Half Day"
                    while current_date <= end_date:
                        if current_date.weekday() < 5:  # Ignore weekends
                            insert_query = f"""
                            INSERT INTO STREAMLIT_APPS.PUBLIC.REP_LEAVE_PTO (NAME, "Hours Worked Text", "Hours Worked", "DATE")
                            VALUES (%s, %s, %s, %s)
                            """
                            cur.execute(insert_query, (selected_name, hours_worked_text, 0 if day_type == 'Full Day' else 0.5, current_date))
                        current_date += timedelta(days=1)
                    conn.commit()

                    main_success_message.success(f"Time off submitted for {selected_name} from {formatted_start_date} to {formatted_end_date}.")
                    time.sleep(5)
                    main_success_message.empty()

                    st.session_state['pto_data'] = fetch_pto_data(conn, selected_name)

    if selected_name != '':
        if 'pto_data' not in st.session_state or st.session_state['pto_data'] is None:
            pto_data = fetch_pto_data(conn, selected_name)
            st.session_state['pto_data'] = pto_data
        else:
            pto_data = st.session_state['pto_data']

        filter_type = st.sidebar.radio('Filter', ('Recent', 'All'), key='filter_type', index=0)

        filtered_pto_data = filter_pto_data(pto_data, filter_type)

        edited_pto_df = pd.DataFrame(filtered_pto_data, columns=["Date", "PTO"])
        edited_pto_df['Date'] = pd.to_datetime(edited_pto_df['Date']).dt.date

        original_pto_df = edited_pto_df.copy()

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
                key='data_editor_sidebar',
            )

        st.sidebar.button("Save Changes", key='save_changes_button', on_click=on_save_changes,
                          args=(selected_name, edited_pto_df, original_pto_df, conn))
