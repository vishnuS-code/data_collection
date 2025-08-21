#db.py

import pandas as pd
import psycopg2
import traceback
import numpy as np


class Execute:
    def __init__(self):
        self.keepalive_kwargs = {
            "keepalives": 1,
            "keepalives_idle": 30,
            "keepalives_interval": 5,
            "keepalives_count": 5,
        }
        self.conn = self.connect()

    def connect(self):
        conn = psycopg2.connect(
            database="central_database",
            user="postgres",
            password="55555",
            host="100.121.194.26",
            port="5432",
            **self.keepalive_kwargs,
        )
        conn.autocommit = True
        return conn

    def select(self, query):
        try:
            cur = self.conn.cursor()
            cur.execute(query)
            rows = [
                {cur.description[i][0]: value for i, value in enumerate(row)}
                for row in cur.fetchall()
            ]
            cur.close()
            return rows

        except Exception as e:
            print("Error:", str(e))
            traceback.print_exc()
            return False

class Fetch_data:
    def __init__(self):
        self.execute = Execute()
        
    def fetch_mill_details(self):
        try:
            query = """
                SELECT milldetails_id,mill_name
                FROM public.mill_details
                ORDER BY milldetails_id ASC           
            """
            result = self.execute.select(query)
            return result
            
        except Exception as e:
            print("Error in fetch_mill_details:", e)
            return None
    
    def fetch_machine_details(self, milldetails_id):
        try:
            query = f"""
                SELECT * 
                FROM public.machine_details
                WHERE milldetails_id = {milldetails_id}
                ORDER BY machinedetail_id ASC 
            """
            result = self.execute.select(query)
            return result
        
        except Exception as e:
            print("Error in fetch_machine_details:", e)
            return None

class RemoteFetchData:
    def __init__(self, ip, database="knitting", user="postgres", password="55555", port=5432):
        self.ip = ip
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.conn = self.connect()

    def connect(self):
        try:
            conn = psycopg2.connect(
                host=self.ip,
                database=self.database,
                user=self.user,
                password=self.password,
                port=self.port
            )
            conn.autocommit = True
            return conn
        except Exception as e:
            print(f"Failed to connect to remote DB at {self.ip}: {e}")
            traceback.print_exc()
            return None

    def fetch_roll_dates(self):
        if not self.conn:
            print(f"No connection to remote DB at {self.ip}")
            return []
        try:
            cur = self.conn.cursor()
            cur.execute("SELECT DISTINCT roll_start_date FROM roll_details WHERE roll_sts_id != 1 ORDER BY roll_start_date ASC;")
            rows = [row[0] for row in cur.fetchall()]
            cur.close()
            return rows
        except Exception as e:
            print(f"Error fetching roll dates from {self.ip}: {e}")
            traceback.print_exc()
            return []

    def fetch_rolls_by_date(self, selected_date):
        if not self.conn:
            print(f"No connection to remote DB at {self.ip}")
            return []
        try:
            cur = self.conn.cursor()
            cur.execute("""
                SELECT roll_number, roll_name ,machineprgdtl_id
                FROM roll_details 
                WHERE roll_start_date = %s AND roll_sts_id != 1
                ORDER BY roll_number ASC;
            """, (selected_date,))
            rows = cur.fetchall()
            cur.close()
            return rows  # [(roll_number, roll_name), ...]
        except Exception as e:
            print(f"Error fetching rolls from {self.ip}: {e}")
            traceback.print_exc()
            return []


    def fetch_machine_program_detail(self, machineprgdtl_id):
        """
        Fetch details of a machine program given machineprgdtl_id.
        Returns a dict or None if not found.
        """
        if not self.conn:
            print(f"No connection to remote DB at {self.ip}")
            return None
        try:
            cur = self.conn.cursor()
            cur.execute("""
                SELECT *
                FROM public.machine_program_details
                WHERE machineprgdtl_id = %s
                LIMIT 1;
            """, (machineprgdtl_id,))
            row = cur.fetchone()
            cur.close()
            if row:
                colnames = [desc[0] for desc in cur.description]
                return dict(zip(colnames, row))
            else:
                return None
        except Exception as e:
            print(f"Error fetching machine program detail from {self.ip}: {e}")
            import traceback
            traceback.print_exc()
            return None
