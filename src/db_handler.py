import mysql.connector
import os
import time
import logging

logger = logging.getLogger("usage-monitor")

class DatabaseHandler:
    def __init__(self, host, user, password, database):
        '''Initialize the connection and ensure the table exists'''

        self.config = {
            'host': host,
            'user': user,
            'password': password,
            'database': database
        }
        self._initialize_schema()

    def _get_connection(self):
        '''Helper func to create a new connection for each operation'''

        while True:
            try:
                conn = mysql.connector.connect(**self.config)
                return conn
            except:
                logger.warning(f"Database connection failed. Retrying in 10 seconds...")
                time.sleep(10)

    def _initialize_schema(self):
        '''Internal method to run the CREATE TABLE IF NOT EXISTS query'''

        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''CREATE TABLE IF NOT EXISTS jobs(
                                job_id VARCHAR(255) PRIMARY KEY, 
                                session_id VARCHAR(255),
                                instrument VARCHAR(255),
                                product_type VARCHAR(255),
                                job_status VARCHAR(255),
                                user_email VARCHAR(255), 
                                user_roles VARCHAR(255),
                                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                            );''')
            conn.commit()
        finally:
            cursor.close()
            conn.close()

    def upsert_job(self, job_data):
        '''
        Takes a dictionary of a job data from the scan_directory() and safely inserts 
        or updates it in the database.
        '''
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''INSERT INTO jobs 
                           (job_id, session_id, instrument, product_type, job_status, user_email, user_roles)
                           VALUES (%s, %s, %s, %s, %s, %s, %s) 
                           ON DUPLICATE KEY UPDATE 
                           job_status = VALUES(job_status);''', 
                           (
                               job_data.get("job_id"),
                               job_data.get("session_id"),
                               job_data.get("instrument"),
                               job_data.get("product_type"),
                               job_data.get("job_status"),
                               job_data.get("user_email"),
                               job_data.get("user_roles")
                           )
            )
            conn.commit()
        finally:
            cursor.close()
            conn.close()