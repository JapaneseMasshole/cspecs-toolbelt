import streamlit as st
from datetime import datetime, timedelta, UTC
from db.database import Database
import pandas as pd
from components.subscriptionhandler import SubscriptionHandler
import threading
from components.logger_config import setup_logging, get_logger

# Set up logging
setup_logging()
logger = get_logger(__name__)

# Create database instance
db = Database('./db/tickcapturejobs.db')
subscription_handler = None
subscription_thread = None

def query_data():
    logger.info("Querying recent jobs")
    try:
        return db.query_recent_jobs()
    except Exception as e:
        logger.error(f"Error querying recent jobs: {e}", exc_info=True)
        st.error("Error querying recent jobs. Please try again.")
        return pd.DataFrame()

def insert_data(job_name, job_startdatetime, job_enddatetime, instruments, fields):
    logger.info(f"Inserting new job: {job_name}")
    try:
        # Store timestamps in UTC without conversion
        db.insert_data(job_name, job_startdatetime, job_enddatetime, instruments, fields)
        db.set_update_flag()
        if 'df' in st.session_state:
            st.session_state.df = query_data()
    except Exception as e:
        logger.error(f"Error inserting data: {e}", exc_info=True)
        raise

def delete_selected_jobs(job_ids):
    logger.info(f"Deleting jobs with IDs: {job_ids}")
    try:
        for job_id in job_ids:
            db.delete_job(job_id)
        # Set the update flag to notify subscription handler
        db.set_update_flag()
        # Force refresh of the dataframe
        if 'df' in st.session_state:
            st.session_state.df = query_data()
    except Exception as e:
        logger.error(f"Error deleting jobs: {e}", exc_info=True)
        raise

def run_subscription_handler():
    global subscription_handler
    logger.info("Starting SubscriptionHandler")
    try:
        if subscription_handler is None:
            subscription_handler = SubscriptionHandler("config/bpipe_config.local.json")
            subscription_handler.start()
        else:
            logger.info("SubscriptionHandler already running")
    except Exception as e:
        logger.error(f"Error starting SubscriptionHandler: {str(e)}", exc_info=True)

def main():
    global subscription_thread
    
    if subscription_thread is None:
        logger.info("Initializing SubscriptionHandler thread")
        subscription_thread = threading.Thread(target=run_subscription_handler)
        subscription_thread.start()

    st.title("Job Management")

    # Top Pane: Display DataFrame with checkboxes
    st.subheader("Recent Jobs")
    
    # Initialize the dataframe if it doesn't exist
    if 'df' not in st.session_state:
        st.session_state.df = query_data()

    # Add a checkbox column to the dataframe
    df_with_checkboxes = st.session_state.df.copy()
    df_with_checkboxes.insert(0, 'Select', False)

    # Use st.data_editor for an editable dataframe with checkboxes
    edited_df = st.data_editor(df_with_checkboxes, hide_index=True, disabled=df_with_checkboxes.columns[1:])

    # Add a refresh button
    if st.button('Refresh Jobs'):
        st.session_state.df = query_data()
        st.rerun()

    # Add a delete button
    if st.button('Delete Selected Jobs'):
        id_column = 'id' if 'id' in edited_df.columns else edited_df.columns[1]
        selected_jobs = edited_df[edited_df['Select']][id_column].tolist()
        if selected_jobs:
            delete_selected_jobs(selected_jobs)
            st.success(f"Selected jobs deleted successfully!")
            st.rerun()
        else:
            st.warning("No jobs selected for deletion.")

    # Bottom Pane: Entry Form
    st.subheader("Add New Job")

    with st.form("job_form"):
        # Input fields with UTC timezone
        job_name = st.text_input("Job Name")
        job_startdatetime = st.date_input("Job Start Date", datetime.now(UTC))
        current_time = datetime.now(UTC)

        # Calculate the nearest quarter of the hour in the future
        minutes = (current_time.minute // 5 + 1) * 5
        if minutes == 60:
            minutes = 0
            current_time = current_time.replace(hour=current_time.hour + 1)
        default_start_time = current_time.replace(minute=minutes, second=0, microsecond=0).time()
        default_end_time = (datetime.combine(datetime.today(), default_start_time).replace(tzinfo=UTC) + timedelta(minutes=15)).time()

        job_starttime = st.time_input("Job Start Time (UTC)", default_start_time, step=timedelta(minutes=5))
        job_enddatetime = st.date_input("Job End Date", datetime.now(UTC))
        job_endtime = st.time_input("Job End Time (UTC)", default_end_time, step=timedelta(minutes=5))
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
            # Make sure combinations are timezone-aware
            job_startdatetime_combined = datetime.combine(job_startdatetime, job_starttime).replace(tzinfo=UTC)
            job_enddatetime_combined = datetime.combine(job_enddatetime, job_endtime).replace(tzinfo=UTC)
            current_time = datetime.now(UTC)
            logger.debug(f"Current time: {current_time}")
            logger.debug(f"Job start datetime: {job_startdatetime_combined}")
            logger.debug(f"Job end datetime: {job_enddatetime_combined}")
            
            if job_startdatetime_combined < current_time:
                st.error("Job Start Time cannot be in the past.")
            elif job_enddatetime_combined <= job_startdatetime_combined:
                st.error("Job End Time must be after Job Start Time.")
            else:
                try:
                    job_startdatetime = job_startdatetime_combined.timestamp()
                    job_enddatetime = job_enddatetime_combined.timestamp()
                    instruments_list = [i.strip() for i in instruments.split('\n') if i.strip()]
                    fields_list = [f.strip() for f in fields.split('\n') if f.strip()]
                    
                    insert_data(job_name, job_startdatetime, job_enddatetime, instruments_list, fields_list)
                    st.success("Job added successfully!")
                    
                    # Refresh the dataframe immediately
                    st.session_state.df = query_data()
                    st.rerun()
                except Exception as e:
                    logger.error(f"Error submitting job: {e}", exc_info=True)
                    st.error(f"Error submitting job: {str(e)}")

    logger.info("Streamlit app rendered")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Application error: {e}", exc_info=True)
        st.error("An unexpected error occurred. Please check the logs.")
