import streamlit as st
from datetime import datetime, timedelta
import snowflake.connector
import pandas as pd
import time

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

# Save changes with corrected date format logic
def save_changes(edited_pto_df, selected_name, conn):
    cur = conn.cursor()

    # Ensure selected_name matches database format (strip or clean if necessary)
    selected_name = selected_name.strip()

    # Debugging: Write what we got from the editor
    st.write("Edited DataFrame from editor:", edited_pto_df)

    # Handle updates and insertions for remaining entries
    for index, row in edited_pto_df.iterrows():
        if pd.isnull(row['Date']):
            st.warning(f"Skipping invalid date in row {index}")
            continue

        # Ensure the Date format matches the database (using consistent formatting)
        row['Date'] = pd.to_datetime(row['Date']).date()

        # Format the date to ensure compatibility with the database (YYYY-MM-DD format)
        formatted_date = row['Date'].strftime('%Y-%m-%d')

        # Ensure the PTO values are updated accordingly
        hours_worked = 0.0 if row['PTO'] == 'Full Day' else 0.5

        # Simplified single update query
        update_query = f"""
        UPDATE STREAMLIT_APPS.PUBLIC.REP_LEAVE_PTO
        SET "Hours Worked Text" = %s, "Hours Worked" = %s
        WHERE TRIM(NAME) = %s AND "DATE" = %s
        """
        
        cur.execute(update_query, (row['PTO'], hours_worked, selected_name, formatted_date))

        # Debugging: Check how many rows were updated
        st.write(f"Rows affected for {formatted_date}:", cur.rowcount)

    conn.commit()
    cur.close()

    # Fetch updated PTO data after changes are saved
    new_pto_data = fetch_pto_data(conn, selected_name)
    st.session_state['pto_data'] = new_pto_data  # Update session state with refreshed data
    st.write("Updated PTO data:", new_pto_data)

    with st.sidebar:
        success_message = st.empty()
        success_message.success("Changes saved successfully!")
        time.sleep(3)
        success_message.empty()

    st.rerun()

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

        if st.button('Submit', key='submit_button'):
            start_date = default_start.date()
            end_date = default_end.date()

            cur = conn.cursor()
            check_query = f"""
            SELECT "DATE" FROM STREAMLIT_APPS.PUBLIC.REP_LEAVE_PTO
            WHERE NAME = %s AND "DATE" BETWEEN %s AND %s
            """
            cur.execute(check_query, (selected_name, start_date, end_date))
            existing_dates = [row[0] for row in cur.fetchall()]

            if existing_dates:
                existing_dates_str = ', '.join([date.strftime('%b %d, %Y') for date in existing_dates])
                st.error(f"PTO already exists for {selected_name} on: {existing_dates_str}.")
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

                st.success(f"Time off submitted for {selected_name} from {start_date} to {end_date} (excluding weekends).")

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
        if st.sidebar.button("Save Changes", key='save_changes_button'):
            save_changes(edited_pto_df, selected_name, conn)
