"""
Transforms and Loads data into the local SQLite3 database
"""

import sqlite3
from databricks import sql
from dotenv import load_dotenv
import csv
import os


# load the csv file and insert into a new sqlite3 database
def load(dataset="data/nfl-wide-receivers.csv"):
    """Transforms and Loads data into the local Databricks database"""

    # prints the full working directory and path
    print(os.getcwd())
    load_dotenv()

    payload = csv.reader(open(dataset, newline="", encoding="UTF-8"), delimiter=",")
    # skips header row
    next(payload)
    with sql.connect(
        server_h=os.getenv("SERVER_HOST"),
        access_token=os.getenv("ACCESS_TOKEN"),
        http_path=os.getenv("HTTP_PATH"),
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SHOW TABLES FROM default LIKE 'jdc_nfl*")
            result = cursor.fetchall()
            if not result:
                cursor.execute(
                    """
                CREATE TABLE jdc_nflReceivers 
                (id INTEGER PRIMARY KEY AUTOINCREMENT, 
                pfr_player_id TEXT,
                player_name TEXT,
                career_try FLOAT,
                career_ranypa FLOAT,
                career_wowy FLOAT,
                bcs_rating FLOAT)
                """
                )

            # insert
            for _, data in payload.iterrows():
                cursor.execute(f"INSERT INTO jdc_nflReceivers VALUES {data}")
        cursor.close()
    return "success"
