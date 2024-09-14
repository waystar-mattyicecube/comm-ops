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

# Initialize connection state in session state
if 'snowflake_connected' not in st.session_state:
    st.session_state['snowflake_connected'] = False
if 'conn' not in st.session_state:
    try:
        st.session_state.conn = snowflake.connector.connect(
            user='mattyicecube',
            password='Mattman1159!',
            account='fna44578.east-us-2.azure',
            warehouse='COMPUTE_WH',
            database='STREAMLIT_APPS',
            schema='PUBLIC'
        )
        st.session_state['snowflake_connected'] = True

        connection_message = st.empty()
        connection_message.success("Connected to Snowflake successfully.")
    except Exception as e:
        st.error(f"Error connecting to Snowflake: {e}")

# Define a function to fetch distinct rep names and cache the results (without passing the connection)
@st.cache_data
def fetch_rep_names(query):
    cur = st.session_state.conn.cursor()
    cur.execute(query)
    return [row[0] for row in cur.fetchall()]

# Define the query for fetching names
query = "SELECT DISTINCT NAME FROM STREAMLIT_APPS.PUBLIC.REP_LEAVE_PTO"

# Fetch rep names and insert default option
names = fetch_rep_names(query)
names.insert(0, 'Select Sales Rep')

# Ensure selected_name persists in session state
if 'selected_name' not in st.session_state:
    st.session_state['selected_name'] = 'Select Sales Rep'

selected_name = st.selectbox('', names, index=names.index(st.session_state['selected_name']))
st.session_state['selected_name'] = selected_name

# If a sales rep is selected, show additional options
if selected_name != 'Select Sales Rep':
    day_type = st.radio('', ['Full Day', 'Half Day'])
    default_start, default_end = datetime.now() - timedelta(days=1), datetime.now()
    refresh_value = timedelta(days=1)

    date_range_string = date_range_picker(
        picker_type=PickerType.date,
        start=default_start, 
        end=default_end,
        key='date_range_picker',
        refresh_button={'is_show': False, 'button_name': 'Refresh Last 1 Days', 'refresh_value': refresh_value}
    )

    if date_range_string:
        start_date, end_date = date_range_string
        start_date = datetime.strptime(start_date, '%Y-%m-%d') if isinstance(start_date, str) else start_date
        end_date = datetime.strptime(end_date, '%Y-%m-%d') if isinstance(end_date, str) else end_date

        formatted_start_date = start_date.strftime('%b %d, %Y')
        formatted_end_date = end_date.strftime('%b %d, %Y')

        st.write(f"{formatted_start_date} - {formatted_end_date}")

        if st.button('Submit'):
            cur = st.session_state.conn.cursor()
            check_query = f"""
            SELECT "DATE" FROM STREAMLIT_APPS.PUBLIC.REP_LEAVE_PTO
            WHERE NAME = %s AND "DATE" BETWEEN %s AND %s
            """
            cur.execute(check_query, (selected_name, start_date, end_date))
            existing_dates = [row[0] for row in cur.fetchall()]

            if existing_dates:
                existing_dates_str = ', '.join([date.strftime('%b %d, %Y') for date in existing_dates])
                error_message = st.empty()
                error_message.error(f"PTO already exists for {selected_name} on: {existing_dates_str}.")
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
                st.session_state.conn.commit()

                success_message = st.empty()
                success_message.success(f"Time off submitted for {selected_name} from {formatted_start_date} to {formatted_end_date} (excluding weekends).")
                st.session_state['submission_success'] = True

# Fetch PTO data for the selected rep and display it
if selected_name != 'Select Sales Rep':
    existing_query = f"""
    SELECT "DATE" FROM STREAMLIT_APPS.PUBLIC.REP_LEAVE_PTO
    WHERE NAME = %s
    """
    cur = st.session_state.conn.cursor()
    cur.execute(existing_query, (selected_name,))
    existing_dates = [row[0] for row in cur.fetchall()]
    existing_dates_set = set(existing_dates)

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

        # Display the filter in the sidebar
        with st.sidebar:
            today = datetime.now().date()
            current_year = today.year
            next_year = current_year + 1

            # Remove the header but keep the filter functionality
            filter_option = st.radio("", ["All", "Recent"], index=1)

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
                error_dates = []

                # Detect deleted rows
                deleted_rows = original_pto_df.loc[~original_pto_df['Date'].isin(edited_pto_df['Date'])]

                # Perform batch delete for all dates that need to be removed with partition pruning
                if not deleted_rows.empty:
                    dates_to_delete = deleted_rows['Date'].tolist()

                    delete_query = f"""
                    DELETE FROM STREAMLIT_APPS.PUBLIC.REP_LEAVE_PTO
                    WHERE NAME = %s AND "DATE" IN ({','.join(['%s' for _ in dates_to_delete])})
                    """
                    cur.execute(delete_query, [selected_name] + dates_to_delete)
                    st.session_state.conn.commit()  # Commit once after the batch delete

                # Handle updates and insertions
                for index, row in edited_pto_df.iterrows():
                    hours_worked = 0.0 if row['PTO'] == 'Full Day' else 0.5

                    if row['Date'].weekday() in [5, 6]:
                        continue

                    if index in pto_df.index:
                        update_query = f"""
                        UPDATE STREAMLIT_APPS.PUBLIC.REP_LEAVE_PTO
                        SET "Hours Worked Text" = %s, "Hours Worked" = %s, "DATE" = %s
                        WHERE NAME = %s AND "DATE" = %s
                        """
                        cur.execute(update_query, (row['PTO'], hours_worked, row['Date'], selected_name, pto_df.loc[index, 'Date']))
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
                    error_message = st.empty()
                    error_message.error(f"Cannot add PTO for the following dates as they already exist: {', '.join(error_dates)}")
                else:
                    st.session_state.conn.commit()
                    success_message = st.empty()
                    success_message.success("Changes saved successfully!")
                    time.sleep(4)  # Keep success message for 4 seconds
                    success_message.empty()
    else:
        with st.sidebar:
            st.write("No PTO records found for the selected sales rep.")
