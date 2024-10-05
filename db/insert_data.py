import sqlite3
import time

def insert_data():
    connection = sqlite3.connect('tickcapturejobs.db')
    cursor = connection.cursor()

    # Insert data into jobs table
    job_name = "Dummy job 1"
    job_startdatetime = int(time.time()) + 15 * 60  # current time + 15 minutes in unix timestamp
    duration = 15
    job_status = "NOT STARTED"

    cursor.execute('''
        INSERT INTO jobs (job_name, job_startdatetime, duration, job_status)
        VALUES (?, ?, ?, ?)
    ''', (job_name, job_startdatetime, duration, job_status))

    # Get the last inserted job_id
    job_id = cursor.lastrowid

    # Insert data into instruments table
    instruments = ["NKY Index", "USDJPY Curncy", "6758 JT Equity"]
    for instrument in instruments:
        cursor.execute('''
            INSERT INTO instruments (instrument_name, job_id)
            VALUES (?, ?)
        ''', (instrument, job_id))

    # Insert data into fields table
    fields = ["MKTDATA_EVENT_TYPE", "MKTDATA_EVENT_SUBTYPE", "BID", "ASK", "LAST_PRICE"]
    for field in fields:
        cursor.execute('''
            INSERT INTO fields (field_name, job_id)
            VALUES (?, ?)
        ''', (field, job_id))

    connection.commit()
    connection.close()

def main():
    insert_data()

if __name__ == '__main__':
    main()