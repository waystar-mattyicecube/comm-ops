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

# Cache the Snowflake connection using @st.cache_resource to persist it throughout the app
@st.cache_resource
def get_snowflake_connection():
    return snowflake.connector.connect(
        user='mattyicecube',
        password='Mattman1159!',
        account='fna44578.east-us-2.azure',
        warehouse='COMPUTE_WH',
        database='STREAMLIT_APPS',
        schema='PUBLIC',
        client_session_keep_alive=True  # Keep the session alive to prevent token expiration
    )

# Cache the function that fetches distinct names from Snowflake using @st.cache_data
@st.cache_data(show_spinner=False)
def fetch_distinct_names(_conn):
    cur = _conn.cursor()
    query = "SELECT DISTINCT NAME FROM STREAMLIT_APPS.PUBLIC.REP_LEAVE_PTO"
    cur.execute(query)
    names = [row[0] for row in cur.fetchall()]
    return names

# Cache PTO data fetch from Snowflake using @st.cache_data
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

# Cache the filtering function for PTO data using @st.cache_data
@st.cache_data
def filter_pto_data(pto_data, filter_type):
    today = datetime.now().date()
    three_months_ago = today - timedelta(days=90)
    one_year_from_now = today + timedelta(days=365)

    if filter_type == 'Recent':
        filtered_data = [row for row in pto_data if (three_months_ago <= row[0] <= one_year_from_now)]
    else:
        filtered_data = pto_data

    return filtered_data

# No caching applied here as this function checks for changes in real-time
def get_changed_rows(edited_pto_df, original_pto_df):
    changed_rows = []
    
    for idx, edited_row in edited_pto_df.iterrows():
        # Check if row exists in original and if any changes are made
        if idx in original_pto_df.index:
            original_row = original_pto_df.loc[idx]
            if edited_row["Date"] != original_row["Date"] or edited_row["PTO"] != original_row["PTO"]:
                changed_rows.append(edited_row)
        else:
            # New row, needs to be checked
            changed_rows.append(edited_row)
    
    return pd.DataFrame(changed_rows)

# No caching is applied here, as this function modifies the Snowflake data
def save_data_editor_changes(edited_pto_df, original_pto_df, selected_name, conn):
    cur = conn.cursor()

    # Find changed rows (updated or new rows)
    changed_rows_df = get_changed_rows(edited_pto_df, original_pto_df)
    
    if not changed_rows_df.empty:
        # Check for duplicates in Snowflake for only changed rows
        changed_dates = changed_rows_df['Date'].tolist()
        check_existing_dates_query = """
        SELECT "DATE" FROM STREAMLIT_APPS.PUBLIC.REP_LEAVE_PTO
        WHERE NAME = %s AND "DATE" IN ({})
        """.format(','.join(['%s'] * len(changed_dates)))

        cur.execute(check_existing_dates_query, [selected_name] + changed_dates)
        duplicate_dates = [row[0] for row in cur.fetchall()]

        if duplicate_dates:
            duplicate_dates_str = ', '.join([date.strftime('%b %d, %Y') for date in duplicate_dates])
            with st.sidebar:
                error_message = st.empty()
                error_message.error(f"PTO already exists for {selected_name} on: {duplicate_dates_str}.")
            time.sleep(5)
            error_message.empty()
            return False  # Return False if duplicate PTO dates exist

    # Detect deleted rows
    deleted_rows_df = original_pto_df.loc[~original_pto_df.index.isin(edited_pto_df.index)]

    # Delete rows from Snowflake
    for _, deleted_row in deleted_rows_df.iterrows():
        delete_query = """
        DELETE FROM STREAMLIT_APPS.PUBLIC.REP_LEAVE_PTO
        WHERE NAME = %s AND "DATE" = %s
        """
        cur.execute(delete_query, (selected_name, deleted_row["Date"]))

    # Insert/update rows in Snowflake
    for idx, edited_row in edited_pto_df.iterrows():
        original_row = original_pto_df.loc[idx] if idx in original_pto_df.index else None

        # Update if row exists, insert if new
        if original_row is not None:
            if edited_row["Date"] != original_row["Date"] or edited_row["PTO"] != original_row["PTO"]:
                update_query = """
                UPDATE STREAMLIT_APPS.PUBLIC.REP_LEAVE_PTO
                SET "DATE" = %s, "Hours Worked Text" = %s
                WHERE NAME = %s AND "DATE" = %s
                """
                cur.execute(update_query, (edited_row["Date"], edited_row["PTO"], selected_name, original_row["Date"]))
        else:
            insert_query = """
            INSERT INTO STREAMLIT_APPS.PUBLIC.REP_LEAVE_PTO (NAME, "DATE", "Hours Worked Text")
            VALUES (%s, %s, %s)
            """
            cur.execute(insert_query, (selected_name, edited_row["Date"], edited_row["PTO"]))

    conn.commit()
    cur.close()

    # Re-fetch the updated PTO data after saving changes
    updated_pto_data = fetch_pto_data(conn, selected_name)
    st.session_state['pto_data'] = updated_pto_data  # Update the session state with the new data

    return True  # Return True if the changes were saved successfully

# Callback function to save data when the "Save Changes" button is clicked
def on_save_changes_callback(selected_name, edited_pto_df, original_pto_df, conn):
    success = save_data_editor_changes(edited_pto_df, original_pto_df, selected_name, conn)
    # Show success message only if the changes were successful
    if success:
        with st.sidebar:
            success_message = st.empty()
            success_message.success("Changes saved successfully!")
            time.sleep(3)
            success_message.empty()

# Reset session state when a new sales rep is selected
def reset_session_state_on_rep_change(selected_name):
    # If there's a change in the selected sales rep, reset the session state to load new data
    if 'last_selected_rep' not in st.session_state or st.session_state['last_selected_rep'] != selected_name:
        st.session_state['last_selected_rep'] = selected_name
        st.session_state['pto_data'] = None  # Reset PTO data to force a refresh

# Establish connection to Snowflake and fetch distinct names
conn = get_snowflake_connection()
names = fetch_distinct_names(conn)
names.insert(0, '')  # Add a placeholder for the selectbox

col1, spacer, col2 = st.columns([8, 0.1, 1])

# Create placeholders for displaying success/error messages for the date picker on the main screen
main_error_message = st.empty()
main_success_message = st.empty()

with col1:
    selected_name = st.selectbox(
        '', 
        names, 
        key='select_rep', 
        format_func=lambda x: 'Select Sales Rep' if x == '' else x
    )

    # Reset session state if the user selects a new sales rep
    reset_session_state_on_rep_change(selected_name)

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
                    main_error_message.error(f"PTO already exists for {selected_name} on: {existing_dates_str}.")
                    time.sleep(5)
                    main_error_message.empty()
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

                    main_success_message.success(f"Time off submitted for {selected_name} from {formatted_start_date} to {formatted_end_date} (excluding weekends).")
                    time.sleep(5)
                    main_success_message.empty()

                    # Fetch updated PTO data and update the editor
                    new_pto_data = fetch_pto_data(conn, selected_name)
                    st.session_state['pto_data'] = new_pto_data

    if selected_name != '':
        # Fetch new PTO data if it's not already in session state or if a new rep is selected
        if 'pto_data' not in st.session_state or st.session_state['pto_data'] is None:
            pto_data = fetch_pto_data(conn, selected_name)
            st.session_state['pto_data'] = pto_data
        else:
            pto_data = st.session_state['pto_data']

        # Add radio buttons for filtering without a header above them
        filter_type = st.sidebar.radio(
            '',
            ('Recent', 'All'),
            key='filter_type',
            index=0  # Default to 'Recent'
        )

        # Filter the data based on the radio button selection
        filtered_pto_data = filter_pto_data(pto_data, filter_type)

        # Prepare the filtered data for display in the editor
        edited_pto_df = pd.DataFrame(filtered_pto_data, columns=["Date", "PTO"])
        edited_pto_df['Date'] = pd.to_datetime(edited_pto_df['Date']).dt.date

        original_pto_df = edited_pto_df.copy()

        # Render the data editor in the sidebar and add the callback for save button
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

        # Save changes button using callback
        st.sidebar.button("Save Changes", key='save_changes_button', on_click=on_save_changes_callback, 
                          args=(selected_name, edited_pto_df, original_pto_df, conn))
