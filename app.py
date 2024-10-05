import sqlite3
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import time

# Function to query data from the database
def query_data():
    connection = sqlite3.connect('./db/tickcapturejobs.db')
    query = '''
        SELECT jobs.job_id, jobs.job_name, 
               strftime('%Y-%m-%d %H:%M:%S', job_startdatetime, 'unixepoch') AS job_startdatetime, 
               job_status, 
               (SELECT COUNT(*) FROM instruments WHERE instruments.job_id = jobs.job_id) AS instrument_count, 
               (SELECT COUNT(*) FROM fields WHERE fields.job_id = jobs.job_id) AS field_count 
        FROM jobs 
        ORDER BY jobs.job_id DESC
        LIMIT 5
    '''
    df = pd.read_sql_query(query, connection)
    connection.close()
    return df

# Function to insert data into the database
def insert_data(job_name, job_startdatetime, job_enddatetime, instruments, fields):
    connection = sqlite3.connect('./db/tickcapturejobs.db')
    cursor = connection.cursor()

    duration = int((job_enddatetime - job_startdatetime) / 60)  # Calculate duration in minutes

    cursor.execute('''
        INSERT INTO jobs (job_name, job_startdatetime, duration, job_status)
        VALUES (?, ?, ?, ?)
    ''', (job_name, job_startdatetime, duration, "NOT STARTED"))

    job_id = cursor.lastrowid

    for instrument in instruments:
        cursor.execute('''
            INSERT INTO instruments (instrument_name, job_id)
            VALUES (?, ?)
        ''', (instrument.strip(), job_id))

    for field in fields:
        cursor.execute('''
            INSERT INTO fields (field_name, job_id)
            VALUES (?, ?)
        ''', (field.strip(), job_id))

    connection.commit()
    connection.close()

# Streamlit UI
st.title("Job Management")

# Top Pane: Display DataFrame
st.subheader("Recent Jobs")
if 'df' not in st.session_state or st.session_state.get('refresh_data', False):
    st.session_state.df = query_data()
    st.session_state.refresh_data = False
st.dataframe(st.session_state.df)

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