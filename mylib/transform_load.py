"""
Transforms and Loads data into the local SQLite3 database
"""

from databricks import sql
from dotenv import load_dotenv
import csv
import os


# load the csv file and insert into databricks database
def load(dataset="data/nfl-wide-receivers.csv"):
    """Transforms and Loads data into the local Databricks database"""

    # prints the full working directory and path
    print(os.getcwd())
    load_dotenv()

    payload = csv.reader(open(dataset, newline="", encoding="UTF-8"), delimiter=",")
    # skips header row
    next(payload)
    host_name = os.getenv("SERVER_HOST")
    token = os.getenv("ACCESS_TOKEN")
    http = os.getenv("HTTP_PATH")
    with sql.connect(
        server_hostname=host_name, http_path=http, access_token=token
    ) as connection:
        c = connection.cursor()

        c.execute(
            """
        CREATE TABLE IF NOT EXISTS jdc_nflReceivers 
        (id INTEGER PRIMARY KEY AUTOINCREMENT, 
        pfr_player_id TEXT,
        player_name TEXT,
        career_try FLOAT,
        career_ranypa FLOAT,
        career_wowy FLOAT,
        bcs_rating FLOAT)
        """
        )
        c.execute("SELECT * form jdg_nflReceivers")
        result = c.fetchall()
        if not result:
            # insert
            '''cursor.executemany(
                """
            INSERT INTO jdc_nflReceivers (
            pfr_player_id,
            player_name,
            career_try,
            career_ranypa,
            career_wowy,
            bcs_rating
            ) VALUES (?,?, ?, ?, ?, ?)""",
                payload,
            )'''
        c.close()
    print("Successfully transformed and loaded to Databricks")
    return "success"


if __name__ == "__main__":
    load()
