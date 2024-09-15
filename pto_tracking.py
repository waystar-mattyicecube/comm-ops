import streamlit as st
from datetime import datetime, timedelta
from streamlit_date_picker import date_range_picker, PickerType
import snowflake.connector
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

# Thread pool for asynchronous database operations
executor = ThreadPoolExecutor(max_workers=5)

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

# Inject custom CSS for button styling
st.markdown(
    """
    <style>
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
    </style>
    """,
    unsafe_allow_html=True
)

# Asynchronous function to save changes
def save_changes(edited_pto_df, original_pto_df, selected_name, conn):
    cur = conn.cursor()
    existing_dates = {row[0] for row in cur.execute(f"SELECT \"DATE\" FROM STREAMLIT_APPS.PUBLIC.REP_LEAVE_PTO WHERE NAME = %s", (selected_name,)).fetchall()}
    new_entries_df = edited_pto_df.loc[~edited_pto_df['Date'].isin(original_pto_df['Date'])]
    
    # Batch insert for new entries
    if not new_entries_df.empty:
        batch_insert_query = "INSERT INTO STREAMLIT_APPS.PUBLIC.REP_LEAVE_PTO (NAME, \"Hours Worked Text\", \"Hours Worked\", \"DATE\") VALUES (%s, %s, %s, %s)"
        rows_to_insert = [(selected_name, row['PTO'], 0.0 if row['PTO'] == 'Full Day' else 0.5, row['Date']) for idx, row in new_entries_df.iterrows()]
        cur.executemany(batch_insert_query, rows_to_insert)

    conn.commit()
    cur.close()

# Snowflake connection details
snowflake_params = {
    'user': 'mattyicecube',
    'password': 'Mattman1159!',
    'account': 'fna44578.east-us-2.azure',
    'warehouse': 'COMPUTE_WH',
    'database': 'STREAMLIT_APPS',
    'schema': 'PUBLIC'
}

# Establish connection to Snowflake if not already connected
if 'conn' not in st.session_state:
    try:
        conn = snowflake.connector.connect(**snowflake_params)
        st.session_state.conn = conn
    except Exception as e:
        st.error(f"Error connecting to Snowflake: {e}")

# Fetch distinct sales rep names from Snowflake, cache for performance
@st.cache_data
def get_sales_reps(conn):
    query = "SELECT DISTINCT NAME FROM STREAMLIT_APPS.PUBLIC.REP_LEAVE_PTO"
    cur = conn.cursor()
    cur.execute(query)
    names = [row[0] for row in cur.fetchall()]
    cur.close()
    return names

# Get sales reps
if st.session_state.get('conn'):
    conn = st.session_state.conn
    names = get_sales_reps(conn)
    names.insert(0, '')  # Add default option

    col1, spacer, col2 = st.columns([8, 0.1, 1])

    with col1:
        selected_name = st.selectbox('', names, format_func=lambda x: 'Select Sales Rep' if x == '' else x)
        if selected_name:
            day_type = st.radio('', ['Full Day', 'Half Day'])
            default_start, default_end = datetime.now() - timedelta(days=1), datetime.now()
            date_range_string = date_range_picker(picker_type=PickerType.date, start=default_start, end=default_end)
            
            if date_range_string and st.button('Submit'):
                start_date, end_date = map(lambda x: datetime.strptime(x, '%Y-%m-%d'), date_range_string)
                future = executor.submit(save_changes, start_date, end_date, selected_name, conn)
                
                with st.sidebar:
                    success_message = st.empty()
                    success_message.success("Time off submitted!")
                    future.result()

    # Display PTO data for selected sales rep
    if selected_name:
        pto_query = f"""
        SELECT "DATE", "Hours Worked Text" FROM STREAMLIT_APPS.PUBLIC.REP_LEAVE_PTO
        WHERE NAME = %s
        ORDER BY "DATE" DESC
        """
        cur = conn.cursor()
        cur.execute(pto_query, (selected_name,))
        pto_data = cur.fetchall()

        if pto_data:
            pto_df = pd.DataFrame(pto_data, columns=["Date", "PTO"])
            pto_df['Date'] = pd.to_datetime(pto_df['Date']).dt.date

            original_pto_df = pto_df.copy()

            with st.sidebar:
                filter_option = st.radio("", ["All", "Recent"], index=1)
                if filter_option == "Recent":
                    today = datetime.now().date()
                    pto_df = pto_df[pto_df['Date'] >= today]

                pto_df = pto_df.sort_values(by='Date', ascending=False).reset_index(drop=True)

                edited_pto_df = st.data_editor(
                    pto_df,
                    num_rows="dynamic",
                    column_config={"Date": st.column_config.Column(width=160), "PTO": st.column_config.SelectboxColumn(options=["Full Day", "Half Day"], width=110)}
                )

                if st.button("Save Changes"):
                    future = executor.submit(save_changes, edited_pto_df, original_pto_df, selected_name, conn)
                    with st.sidebar:
                        success_message = st.empty()
                        success_message.success("Changes saved successfully!")
                    future.result()
