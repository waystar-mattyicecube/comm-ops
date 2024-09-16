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

if selected_name != '':
    pto_data = fetch_pto_data(conn, selected_name)

    if pto_data:
        pto_df = pd.DataFrame(pto_data, columns=["Date", "PTO"])

        # Convert 'Date' column to datetime type to avoid 'dt' accessor issues
        pto_df['Date'] = pd.to_datetime(pto_df['Date'], errors='coerce').dt.date

        original_pto_df = pto_df.copy()

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

        st.button(
            "Save Changes", 
            key='save_changes_button', 
            on_click=save_changes, 
            args=(edited_pto_df, original_pto_df, selected_name, conn)
        )
