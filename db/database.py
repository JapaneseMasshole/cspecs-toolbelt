import sqlite3
from typing import List, Tuple, Optional
import pandas as pd

class Database:
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = None
        self.connect()

    def connect(self):
        try:
            self.conn = sqlite3.connect(self.db_path)
            print(f"Connected to database: {self.db_path}")
        except sqlite3.Error as e:
            print(f"Error connecting to database: {e}")

    def _execute_query(self, query: str, params: Tuple = ()) -> List[Tuple]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            return cursor.fetchall()

    def _execute_command(self, command: str, params: Tuple = ()) -> None:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(command, params)
            conn.commit()

    def query_recent_jobs(self, limit: int = 5) -> pd.DataFrame:
        query = '''
            SELECT jobs.job_id, jobs.job_name, 
                   strftime('%Y-%m-%d %H:%M:%S', job_startdatetime, 'unixepoch') AS job_startdatetime, 
                   job_status, 
                   (SELECT COUNT(*) FROM instruments WHERE instruments.job_id = jobs.job_id) AS instrument_count, 
                   (SELECT COUNT(*) FROM fields WHERE fields.job_id = jobs.job_id) AS field_count 
            FROM jobs 
            ORDER BY jobs.job_id DESC
            LIMIT ?
        '''
        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql_query(query, conn, params=(limit,))
        return df

    def insert_job(self, job_name: str, job_startdatetime: int, duration: int, job_status: str) -> int:
        query = '''
            INSERT INTO jobs (job_name, job_startdatetime, duration, job_status)
            VALUES (?, ?, ?, ?)
        '''
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(query, (job_name, job_startdatetime, duration, job_status))
            conn.commit()
            return cursor.lastrowid

    def insert_instruments(self, instruments: List[str], job_id: int) -> None:
        query = '''
            INSERT INTO instruments (instrument_name, job_id)
            VALUES (?, ?)
        '''
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            for instrument in instruments:
                cursor.execute(query, (instrument.strip(), job_id))
            conn.commit()

    def insert_fields(self, fields: List[str], job_id: int) -> None:
        query = '''
            INSERT INTO fields (field_name, job_id)
            VALUES (?, ?)
        '''
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            for field in fields:
                cursor.execute(query, (field.strip(), job_id))
            conn.commit()

    def insert_data(self, job_name: str, job_startdatetime: int, job_enddatetime: int, 
                    instruments: List[str], fields: List[str]) -> None:
        duration = int((job_enddatetime - job_startdatetime) / 60)  # Calculate duration in minutes
        job_id = self.insert_job(job_name, job_startdatetime, duration, "NOT STARTED")
        self.insert_instruments(instruments, job_id)
        self.insert_fields(fields, job_id)

    def delete_job(self, job_id):
        query = "DELETE FROM jobs WHERE job_id = ?"
        with self.conn:
            self.conn.execute(query, (job_id,))

    def __del__(self):
        if self.conn:
            self.conn.close()
            print("Database connection closed.")