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

# Initialize session state variables
if 'snowflake_connected' not in st.session_state:
    st.session_state['snowflake_connected'] = False
if 'conn' not in st.session_state:
    st.session_state['conn'] = None
if 'selected_name' not in st.session_state:
    st.session_state['selected_name'] = 'Select Sales Rep'
if 'date_range' not in st.session_state:
    st.session_state['date_range'] = None

# Snowflake connection details
snowflake_user = 'your_user'
snowflake_password = 'your_password'
snowflake_account = 'your_account'
snowflake_warehouse = 'your_warehouse'
snowflake_database = 'your_database'
snowflake_schema = 'your_schema'

# Establish connection to Snowflake if not already connected
if not st.session_state['snowflake_connected']:
    try:
        st.session_state['conn'] = snowflake.connector.connect(
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

# Reuse the Snowflake connection from session state
conn = st.session_state['conn']

# Fetch distinct values for the NAME column
query = "SELECT DISTINCT NAME FROM STREAMLIT_APPS.PUBLIC.REP_LEAVE_PTO"
cur = conn.cursor()
cur.execute(query)
names = [row[0] for row in cur.fetchall()]
names.insert(0, 'Select Sales Rep')

# Layout with a wider first column, spacer, and a second column
col1, spacer, col2 = st.columns([8, 0.1, 1])

# In the first column, display the dropdown and inputs for PTO submission
with col1:
    selected_name = st.selectbox('Select Sales Rep', names, index=names.index(st.session_state['selected_name']))
    st.session_state['selected_name'] = selected_name

    if selected_name != 'Select Sales Rep':
        day_type = st.radio('', ['Full Day', 'Half Day'])

        if 'date_range' not in st.session_state or st.session_state['date_range'] is None:
            default_start, default_end = datetime.now() - timedelta(days=1), datetime.now()
            st.session_state['date_range'] = date_range_picker(picker_type=PickerType.date,
                                                               start=default_start, end=default_end)

        date_range = st.session_state['date_range']
        if date_range:
            start_date, end_date = date_range
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
                    st.success(f"Time off submitted for {selected_name} from {formatted_start_date} to {formatted_end_date} (excluding weekends).")

# Move PTO data display to the sidebar and allow editing
if selected_name != 'Select Sales Rep':
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

            filter_option = st.radio("", ["All", "Recent"], index=1)

            if filter_option == "Recent":
                pto_df = pto_df[pto_df['Date'].apply(lambda x: x.year in [current_year, next_year])]

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

            if st.button("Save Changes"):
                error_dates = []
                deleted_rows = original_pto_df.loc[~original_pto_df['Date'].isin(edited_pto_df['Date'])]

                # Batch delete
                if not deleted_rows.empty:
                    dates_to_delete = deleted_rows['Date'].tolist()
                    delete_query = f"""
                    DELETE FROM STREAMLIT_APPS.PUBLIC.REP_LEAVE_PTO
                    WHERE NAME = %s AND "DATE" IN ({','.join(['%s' for _ in dates_to_delete])})
                    """
                    cur.execute(delete_query, [selected_name] + dates_to_delete)
                    conn.commit()

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
                        if row['Date'] in existing_dates:
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
    else:
        with st.sidebar:
            st.write("No PTO records found for the selected sales rep.")

cur.close()
conn.close()
