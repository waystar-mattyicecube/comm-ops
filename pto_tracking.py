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

# Callback function to save changes with duplicate check only for new entries
def save_changes(edited_pto_df, original_pto_df, selected_name, conn):
    if conn is None:
        with st.sidebar:
            st.error("Database connection is not available.")
        return

    cur = conn.cursor()

    # Get existing PTO dates from Snowflake for the selected user
    check_query = """
    SELECT "DATE" FROM STREAMLIT_APPS.PUBLIC.REP_LEAVE_PTO
    WHERE NAME = %s
    """
    cur.execute(check_query, (selected_name,))
    existing_dates = [row[0] for row in cur.fetchall()]

    # Check for duplicates within the edited PTO DataFrame itself
    duplicate_dates_in_df = edited_pto_df['Date'][edited_pto_df['Date'].duplicated(keep=False)].drop_duplicates()

    if not duplicate_dates_in_df.empty:
        # Format the distinct duplicate dates for display
        duplicate_dates_str = ', '.join([date.strftime('%b %d, %Y') for date in duplicate_dates_in_df])

        with st.sidebar:
            st.error(f"PTO records already occurs on the following dates: {duplicate_dates_str}. Revise your entry.")
        return  # Exit the function if duplicates exist within the DataFrame itself

    # Detect new dates added by the user
    new_entries_df = edited_pto_df.loc[~edited_pto_df['Date'].isin(original_pto_df['Date'])]

    # Check for duplicates with the existing PTO data in Snowflake
    if not new_entries_df.empty:
        new_dates = new_entries_df['Date'].tolist()
        duplicate_dates = [date for date in new_dates if date in existing_dates]

        if duplicate_dates:
            # Format the conflicting dates to be more user-friendly
            conflicting_dates_str = ', '.join([date.strftime('%b %d, %Y') for date in duplicate_dates])

            with st.sidebar:
                st.error(f"Cannot save. The following PTO dates already exist for {selected_name}: {conflicting_dates_str}.")
            return  # Prevent submission if duplicates are found

    # Detect deleted rows
    deleted_rows = original_pto_df.loc[~original_pto_df['Date'].isin(edited_pto_df['Date'])]

    # Perform batch delete for all dates that need to be removed
    if not deleted_rows.empty:
        dates_to_delete = deleted_rows['Date'].tolist()

        delete_query = f"""
        DELETE FROM STREAMLIT_APPS.PUBLIC.REP_LEAVE_PTO
        WHERE NAME = %s AND "DATE" IN ({','.join(['%s' for _ in dates_to_delete])})
        """
        cur.execute(delete_query, [selected_name] + dates_to_delete)
        conn.commit()

    # Handle updates and insertions for remaining entries
    for index, row in edited_pto_df.iterrows():
        # Ensure the Date column is properly converted to datetime
        if pd.isnull(row['Date']):
            with st.sidebar:
                st.warning(f"Skipping invalid date in row {index}")
            continue

        # Convert the date to datetime if necessary
        if isinstance(row['Date'], str):
            row['Date'] = pd.to_datetime(row['Date'])

        # Check if the date is valid and avoid weekends
        if row['Date'].weekday() in [5, 6]:
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

    # Display success message below the save changes button in the sidebar for 5 seconds
    with st.sidebar:
        success_message = st.empty()
        success_message.success("Changes saved successfully!")
        time.sleep(5)
        success_message.empty()

# Snowflake connection details
snowflake_user = 'mattyicecube'
snowflake_password = 'Mattman1159!'
snowflake_account = 'fna44578.east-us-2.azure'
snowflake_warehouse = 'COMPUTE_WH'
snowflake_database = 'STREAMLIT_APPS'
snowflake_schema = 'PUBLIC'

# Establish connection to Snowflake if not already connected
if 'conn' not in st.session_state:
    try:
        st.session_state.conn = snowflake.connector.connect(
            user=snowflake_user,
            password=snowflake_password,
            account=snowflake_account,
            warehouse=snowflake_warehouse,
            database=snowflake_database,
            schema=snowflake_schema
        )
        st.session_state['snowflake_connected'] = True
        st.success("Connected to Snowflake successfully.")
    except Exception as e:
        st.error(f"Error connecting to Snowflake: {e}")

# Fetch distinct values for the NAME column in STREAMLIT_APPS.PUBLIC.REP_LEAVE_PTO
if st.session_state.get('snowflake_connected'):
    conn = st.session_state.conn
    query = "SELECT DISTINCT NAME FROM STREAMLIT_APPS.PUBLIC.REP_LEAVE_PTO"
    cur = conn.cursor()
    cur.execute(query)
    names = [row[0] for row in cur.fetchall()]

    # Add default option for the dropdown
    names.insert(0, '')

    # Layout with a wider first column, spacer, and a second column
    col1, spacer, col2 = st.columns([8, 0.1, 1])

    # In the first column, display the dropdown and inputs for PTO submission (no header for selectbox)
    with col1:
        # Add a placeholder in the selectbox with the format_func
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

                # Submit button for PTO submission
                if st.button('Submit', key='submit_button'):
                    check_query = f"""
                    SELECT "DATE" FROM STREAMLIT_APPS.PUBLIC.REP_LEAVE_PTO
                    WHERE NAME = %s AND "DATE" BETWEEN %s AND %s
                    """
                    cur.execute(check_query, (selected_name, start_date, end_date))
                    existing_dates = [row[0] for row in cur.fetchall()]

                    if existing_dates:
                        existing_dates_str = ', '.join([date.strftime('%b %d, %Y') for date in existing_dates])
                        with st.sidebar:
                            error_message = st.empty()
                            error_message.error(f"PTO already exists for {selected_name} on: {existing_dates_str}.")
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
                                cur.execute(insert_query, (selected_name, hours_worked_text, hours_worked, current_date))
                            current_date += timedelta(days=1)
                        conn.commit()

                        with st.sidebar:
                            success_message = st.empty()
                            success_message.success(f"Time off submitted for {selected_name} from {formatted_start_date} to {formatted_end_date} (excluding weekends).")
                        time.sleep(3)
                        success_message.empty()

    # Display PTO data and allow edits if a sales rep is selected
    if selected_name != '':
        pto_query = f"""
        SELECT "DATE", "Hours Worked Text" FROM STREAMLIT_APPS.PUBLIC.REP_LEAVE_PTO
        WHERE NAME = %s
        ORDER BY "DATE" DESC
        """
        cur.execute(pto_query, (selected_name,))
        pto_data = cur.fetchall()

        if pto_data:
            pto_df = pd.DataFrame(pto_data, columns=["Date", "PTO"])
            pto_df['Date'] = pd.to_datetime(pto_df['Date']).dt.date

            # Store the original PTO data before editing
            original_pto_df = pto_df.copy()

            # Sidebar for PTO data display and filtering
            with st.sidebar:
                # No header, only radio buttons for filtering
                today = datetime.now().date()
                current_year = today.year
                next_year = current_year + 1

                filter_option = st.radio("", ["All", "Recent"], index=1, key="filter_option")

                if filter_option == "Recent":
                    pto_df = pto_df[pto_df['Date'].apply(lambda x: x.year in [current_year, next_year])]

                # Sort and reset index for better display
                pto_df = pto_df.reset_index(drop=True)
                pto_df = pto_df.sort_values(by='Date', ascending=False)

                # Data editor for editing PTO entries, including a selectbox for PTO with custom widths
                st.write("Edit PTO Entries:")
                edited_pto_df = st.data_editor(
                    pto_df,
                    num_rows="dynamic",
                    column_config={
                        "Date": st.column_config.Column(label="Date", width=160),
                        "PTO": st.column_config.SelectboxColumn(
                            label="PTO", options=["Full Day", "Half Day"], width=110, required=True
                        ),
                    },
                    hide_index=True
                )

                # Save changes button with a callback to update the data
                st.button(
                    "Save Changes", 
                    key='save_changes_button', 
                    on_click=save_changes, 
                    args=(edited_pto_df, original_pto_df, selected_name, conn)
                )
