import sqlite3
from typing import List, Tuple, Optional
import pandas as pd
import json

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

    def query_active_jobs(self, current_time):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT j.job_id, j.job_name, j.job_startdatetime, j.job_startdatetime + j.duration * 60 as job_enddatetime,
                   GROUP_CONCAT(DISTINCT i.instrument_name) as instruments,
                   GROUP_CONCAT(DISTINCT f.field_name) as fields
            FROM jobs j
            LEFT JOIN instruments i ON j.job_id = i.job_id
            LEFT JOIN fields f ON j.job_id = f.job_id
            WHERE j.job_startdatetime <= ? AND j.job_startdatetime + j.duration * 60 > ?
            GROUP BY j.job_id
        """, (current_time, current_time))
        rows = cursor.fetchall()
        conn.close()

        return [
            {
                'id': row[0],
                'job_name': row[1],
                'job_startdatetime': row[2],
                'job_enddatetime': row[3],
                'instruments': row[4].split(',') if row[4] else [],
                'fields': row[5].split(',') if row[5] else []
            }
            for row in rows
        ]

    def __del__(self):
        if self.conn:
            self.conn.close()
            print("Database connection closed.")

    def set_update_flag(self):
        self._execute_command("UPDATE metadata SET value = 1 WHERE key = 'update_flag'")

    def check_update_flag(self):
        result = self._execute_query("SELECT value FROM metadata WHERE key = 'update_flag'")
        return result[0][0] == 1 if result else False

    def clear_update_flag(self):
        self._execute_command("UPDATE metadata SET value = 0 WHERE key = 'update_flag'")
