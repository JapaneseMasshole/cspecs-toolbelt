import streamlit as st
from datetime import datetime, timedelta
from db.database import Database
import pandas as pd

db = Database('./db/tickcapturejobs.db')

def query_data():
    return db.query_recent_jobs()

def insert_data(job_name, job_startdatetime, job_enddatetime, instruments, fields):
    db.insert_data(job_name, job_startdatetime, job_enddatetime, instruments, fields)

def delete_selected_jobs(job_ids):
    for job_id in job_ids:
        db.delete_job(job_id)

def main():
    st.title("Job Management")

    # Top Pane: Display DataFrame with checkboxes
    st.subheader("Recent Jobs")
    if 'df' not in st.session_state or st.session_state.get('refresh_data', False):
        st.session_state.df = query_data()
        st.session_state.refresh_data = False

    # Add a checkbox column to the dataframe
    df_with_checkboxes = st.session_state.df.copy()
    df_with_checkboxes.insert(0, 'Select', False)

    # Use st.data_editor for an editable dataframe with checkboxes
    edited_df = st.data_editor(df_with_checkboxes, hide_index=True, disabled=df_with_checkboxes.columns[1:])

    # Add a delete button
    if st.button('Delete Selected Jobs'):
        # Check if 'id' column exists, if not use the first column (excluding 'Select')
        id_column = 'id' if 'id' in edited_df.columns else edited_df.columns[1]
        selected_jobs = edited_df[edited_df['Select']][id_column].tolist()
        if selected_jobs:
            delete_selected_jobs(selected_jobs)
            st.session_state.refresh_data = True
            st.success(f"Selected jobs deleted successfully!")
            st.rerun()
        else:
            st.warning("No jobs selected for deletion.")

    # Bottom Pane: Entry Form
    st.subheader("Add New Job")

    with st.form("job_form"):
        job_name = st.text_input("Job Name")
        job_startdatetime = st.date_input("Job Start Date", datetime.now())
        current_time = datetime.now()

        # Calculate the nearest quarter of the hour in the future
        minutes = (current_time.minute // 15 + 1) * 15
        if minutes == 60:
            minutes = 0
            current_time = current_time.replace(hour=current_time.hour + 1)
        default_start_time = current_time.replace(minute=minutes, second=0, microsecond=0).time()
        default_end_time = (datetime.combine(datetime.today(), default_start_time) + timedelta(minutes=15)).time()

        job_starttime = st.time_input("Job Start Time", default_start_time)
        job_enddatetime = st.date_input("Job End Date", datetime.now())
        job_endtime = st.time_input("Job End Time", default_end_time)
        instruments = st.text_area("Instruments (one per line)", height=100)
        fields = st.text_area("Fields (one per line)", height=100)

        submitted = st.form_submit_button("Submit")

    # Display persistent message
    if 'success_message' in st.session_state:
        st.success(st.session_state.success_message)
        # Clear the message after displaying it
        del st.session_state.success_message

    if submitted:
        if not job_name or not job_startdatetime or not job_starttime or not job_enddatetime or not job_endtime or not instruments or not fields:
            st.error("All fields are mandatory.")
        else:
            job_startdatetime_combined = datetime.combine(job_startdatetime, job_starttime)
            job_enddatetime_combined = datetime.combine(job_enddatetime, job_endtime)
            current_time = datetime.now()
            if job_startdatetime_combined < current_time:
                st.error("Job Start Time cannot be in the past.")
            elif job_enddatetime_combined <= job_startdatetime_combined:
                st.error("Job End Time must be after Job Start Time.")
            else:
                job_startdatetime = job_startdatetime_combined.timestamp()
                job_enddatetime = job_enddatetime_combined.timestamp()
                instruments_list = instruments.split('\n')
                fields_list = fields.split('\n')
                insert_data(job_name, job_startdatetime, job_enddatetime, instruments_list, fields_list)
                st.session_state.success_message = "Job added successfully!"
                st.session_state.refresh_data = True
                st.rerun()

    # Check if we need to refresh the dataframe
    if st.session_state.get('refresh_data', False):
        st.session_state.df = query_data()
        st.session_state.refresh_data = False

if __name__ == "__main__":
    main()
