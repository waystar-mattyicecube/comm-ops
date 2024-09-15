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

# Callback function to save changes with duplicate check only for new entries
def save_changes(edited_pto_df, original_pto_df, selected_name, conn, existing_dates_set):
    if conn is None or conn.is_closed():
        st.error("Database connection is not available.")
        return

    cur = conn.cursor()

    error_dates = []

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
        conn.commit()  # Commit once after the batch delete

    # Handle updates and insertions for remaining entries
    for index, row in edited_pto_df.iterrows():
        hours_worked = 0.0 if row['PTO'] == 'Full Day' else 0.5

        if row['Date'].weekday() in [5, 6]:  # Avoid weekends
            continue

        if index in original_pto_df.index:
            update_query = f"""
            UPDATE STREAMLIT_APPS.PUBLIC.REP_LEAVE_PTO
            SET "Hours Worked Text" = %s, "Hours Worked" = %s, "DATE" = %s
            WHERE NAME = %s AND "DATE" = %s
            """
            cur.execute(update_query, (row['PTO'], hours_worked, row['Date'], selected_name, original_pto_df.loc[index, 'Date']))
        else:
            if row['Date'] in existing_dates_set:
                error_dates.append(row['Date'].strftime('%b %d, %Y'))
            else:
                insert_query = f"""
                INSERT INTO STREAMLIT_APPS.PUBLIC.REP_LEAVE_PTO (NAME, "Hours Worked Text", "Hours Worked", "DATE")
                VALUES (%s, %s, %s, %s)
                """
                cur.execute(insert_query, (selected_name, row['PTO'], hours_worked, row['Date']))

    if error_dates:
        st.error(f"Cannot add PTO for the following dates as they already exist: {', '.join(error_dates)}")
    else:
        conn.commit()
        st.success("Changes saved successfully!")

    cur.close()

# Snowflake connection details
snowflake_user = 'mattyicecube'
snowflake_password = 'Mattman1159!'
snowflake_account = 'fna44578.east-us-2.azure'
snowflake_warehouse = 'COMPUTE_WH'
snowflake_database = 'STREAMLIT_APPS'
snowflake_schema = 'PUBLIC'

# Establish connection to Snowflake if not already connected
if 'conn' not in st.session_state or st.session_state.conn.is_closed():
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
        # Add a placeholder in the selectbox with the format_func and add label_visibility for the label issue
        selected_name = st.selectbox(
            'Select Sales Rep', 
            names, 
            key='select_rep', 
            format_func=lambda x: 'Select Sales Rep' if x == '' else x,
            label_visibility="collapsed"
        )

        if selected_name != '':
            # Fetch PTO records for the selected sales rep
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

                existing_dates_set = set(original_pto_df['Date'])

                with st.sidebar:
                    today = datetime.now().date()
                    current_year = today.year
                    next_year = current_year + 1

                    # Radio button to filter PTO entries, ensure label visibility to avoid warnings
                    filter_option = st.radio("Filter PTO Entries", ["All", "Recent"], index=1, label_visibility="collapsed")

                    if filter_option == "Recent":
                        pto_df = pto_df[pto_df['Date'].apply(lambda x: x.year in [current_year, next_year])]

                    # Reset index to remove the row index
                    pto_df = pto_df.reset_index(drop=True)
                    pto_df = pto_df.sort_values(by='Date', ascending=False)

                    # Display the DataFrame in the sidebar with editing capabilities and hide index
                    st.write("Edit/Delete Entries:")
                    edited_pto_df = st.data_editor(
                        pto_df,
                        num_rows="dynamic",
                        column_config={
                            "PTO": st.column_config.SelectboxColumn(
                                label="PTO", options=["Full Day", "Half Day"], required=True, width=100
                            ),
                            "Date": st.column_config.Column(
                                label="Date", required=True, width=150
                            )
                        },
                        hide_index=True  # Hide the row indexes
                    )

                    # Button to save changes
                    if st.button("Save Changes"):
                        save_changes(edited_pto_df, original_pto_df, selected_name, conn, existing_dates_set)
            else:
                st.sidebar.write("No PTO records found for the selected sales rep.")

cur.close()
conn.close()
