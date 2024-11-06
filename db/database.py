import sqlite3
from typing import List, Tuple, Optional, Dict, Any
import pandas as pd
import json
import threading
from components.logger_config import get_logger

logger = get_logger(__name__)

class Database:
    def __init__(self, db_path: str):
        self.db_path: str = db_path
        self._local = threading.local()

    @property
    def conn(self) -> sqlite3.Connection:
        if not hasattr(self._local, 'conn'):
            try:
                self._local.conn = sqlite3.connect(self.db_path)
                logger.debug(f"Created new database connection for thread {threading.get_ident()}")
            except sqlite3.Error as e:
                logger.error(f"Error creating database connection: {e}", exc_info=True)
                raise
        return self._local.conn

    def _execute_query(self, query: str, params: Tuple = ()) -> List[Tuple]:
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, params)
            #logger.debug(f"Executed query: {query}")
            return cursor.fetchall()
        except sqlite3.Error as e:
            logger.error(f"Error executing query: {query}. Error: {e}", exc_info=True)
            raise

    def _execute_command(self, command: str, params: Tuple = ()) -> None:
        try:
            cursor = self.conn.cursor()
            cursor.execute(command, params)
            self.conn.commit()
            logger.debug(f"Executed command: {command}")
        except sqlite3.Error as e:
            logger.error(f"Error executing command: {command}. Error: {e}", exc_info=True)
            raise

    def query_recent_jobs(self, limit: int = 5) -> pd.DataFrame:
        logger.info(f"Querying {limit} recent jobs")
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
        try:
            with sqlite3.connect(self.db_path) as conn:
                df = pd.read_sql_query(query, conn, params=(limit,))
            return df
        except (sqlite3.Error, pd.io.sql.DatabaseError) as e:
            logger.error(f"Error querying recent jobs: {e}", exc_info=True)
            raise

    def insert_job(self, job_name: str, job_startdatetime: int, duration: int, job_status: str) -> int:
        logger.info(f"Inserting new job: {job_name}")
        query = '''
            INSERT INTO jobs (job_name, job_startdatetime, duration, job_status)
            VALUES (?, ?, ?, ?)
        '''
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, (job_name, job_startdatetime, duration, job_status))
            return cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error inserting job: {job_name}. Error: {e}", exc_info=True)
            raise

    def insert_instruments(self, instruments: List[str], job_id: int) -> None:
        query = '''
            INSERT INTO instruments (instrument_name, job_id)
            VALUES (?, ?)
        '''
        try:
            cursor = self.conn.cursor()
            for instrument in instruments:
                cursor.execute(query, (instrument.strip(), job_id))
        except sqlite3.Error as e:
            logger.error(f"Error inserting instruments for job_id {job_id}. Error: {e}", exc_info=True)
            raise

    def insert_fields(self, fields: List[str], job_id: int) -> None:
        query = '''
            INSERT INTO fields (field_name, job_id)
            VALUES (?, ?)
        '''
        try:
            cursor = self.conn.cursor()
            for field in fields:
                cursor.execute(query, (field.strip(), job_id))
        except sqlite3.Error as e:
            logger.error(f"Error inserting fields for job_id {job_id}. Error: {e}", exc_info=True)
            raise

    def insert_data(self, job_name: str, job_startdatetime: int, job_enddatetime: int, 
                    instruments: List[str], fields: List[str]) -> None:
        try:
            duration = int((job_enddatetime - job_startdatetime) / 60)  # Calculate duration in minutes
            job_id = self.insert_job(job_name, job_startdatetime, duration, "NOT STARTED")
            self.insert_instruments(instruments, job_id)
            self.insert_fields(fields, job_id)
        except Exception as e:
            logger.error(f"Error inserting data for job: {job_name}. Error: {e}", exc_info=True)
            raise

    def delete_job(self, job_id: int) -> None:
        logger.info(f"Deleting job with ID: {job_id}")
        query = "DELETE FROM jobs WHERE job_id = ?"
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, (job_id,))
        except sqlite3.Error as e:
            logger.error(f"Error deleting job with ID {job_id}. Error: {e}", exc_info=True)
            raise

    def query_active_jobs(self, current_time: float) -> List[Dict[str, Any]]:
        logger.info(f"Querying active jobs at time: {current_time}")
        try:
            cursor = self.conn.cursor()
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
        except sqlite3.Error as e:
            logger.error(f"Error querying active jobs. Error: {e}", exc_info=True)
            raise

    def close(self) -> None:
        if hasattr(self._local, 'conn'):
            try:
                self._local.conn.close()
                delattr(self._local, 'conn')
                logger.info(f"Closed database connection for thread {threading.get_ident()}")
            except sqlite3.Error as e:
                logger.error(f"Error closing database connection: {e}", exc_info=True)

    def __del__(self) -> None:
        self.close()

    def set_update_flag(self) -> None:
        logger.info("Setting update flag")
        try:
            self._execute_command("UPDATE metadata SET value = 1 WHERE key = 'update_flag'")
        except sqlite3.Error as e:
            logger.error(f"Error setting update flag: {e}", exc_info=True)
            raise

    def check_update_flag(self) -> bool:
        try:
            result = self._execute_query("SELECT value FROM metadata WHERE key = 'update_flag'")
            flag_value = result[0][0] == 1 if result else False
            logger.debug(f"Checked update flag, value: {flag_value}")
            return flag_value
        except sqlite3.Error as e:
            logger.error(f"Error checking update flag: {e}", exc_info=True)
            raise

    def clear_update_flag(self) -> None:
        logger.info("Clearing update flag")
        try:
            self._execute_command("UPDATE metadata SET value = 0 WHERE key = 'update_flag'")
        except sqlite3.Error as e:
            logger.error(f"Error clearing update flag: {e}", exc_info=True)
            raise
