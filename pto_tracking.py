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

# Callback function to save changes
def save_changes(edited_pto_df, original_pto_df, selected_name, conn, cur):
    error_dates = []

    # Detect deleted rows
    deleted_rows = original_pto_df.loc[~original_pto_df['Date'].isin(edited_pto_df['Date'])]

    # Perform batch delete for all dates that need to be removed with partition pruning
    if not deleted_rows.empty:
        dates_to_delete = deleted_rows['Date'].tolist()

        # Create the DELETE query with batch deletion using IN clause and date pruning
        delete_query = f"""
        DELETE FROM STREAMLIT_APPS.PUBLIC.REP_LEAVE_PTO
        WHERE NAME = %s AND "DATE" IN ({','.join(['%s' for _ in dates_to_delete])})
        """
        cur.execute(delete_query, [selected_name] + dates_to_delete)
        conn.commit()  # Commit once after the batch delete

    # Handle updates and insertions
    for index, row in edited_pto_df.iterrows():
        hours_worked = 0.0 if row['PTO'] == 'Full Day' else 0.5

        if row['Date'].weekday() in [5, 6]:
            continue

        update_query = f"""
        UPDATE STREAMLIT_APPS.PUBLIC.REP_LEAVE_PTO
        SET "Hours Worked Text" = %s, "Hours Worked" = %s, "DATE" = %s
        WHERE NAME = %s AND "DATE" = %s
        """
        cur.execute(update_query, (row['PTO'], hours_worked, row['Date'], selected_name, row['Date']))
    
    conn.commit()
    st.success("Changes saved successfully!")

# Initialize connection state in session state
if 'snowflake_connected' not in st.session_state:
    st.session_state['snowflake_connected'] = False

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
        st.success("Connected to Snowflake successfully.")
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

# Select sales rep with a unique key
selected_name = st.selectbox('Select Sales Rep', names, key='select_rep')

# Display PTO data and allow edits if a sales rep is selected
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

        # Display the data editor
        edited_pto_df = st.data_editor(
            pto_df,
            num_rows="dynamic",
            key='pto_editor',  # Unique key for data editor
        )

        # Save changes button with a callback to update the data
        st.button(
            "Save Changes", 
            key='save_changes_button', 
            on_click=save_changes, 
            args=(edited_pto_df, original_pto_df, selected_name, conn, cur)
        )
