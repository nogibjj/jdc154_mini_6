"""
Transforms and Loads data into the local SQLite3 database
"""

import sqlite3
import csv
import os


# load the csv file and insert into a new sqlite3 database
def load(dataset="data/nfl-wide-receivers.csv"):
    """Transforms and Loads data into the local SQLite3 database"""

    # prints the full working directory and path
    print(os.getcwd())
    payload = csv.reader(open(dataset, newline="", encoding="UTF-8"), delimiter=",")
    # skips header row
    next(payload)
    conn = sqlite3.connect("nflReceivers.db")
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS nflReceivers")
    c.execute(
        """
        CREATE TABLE nflReceivers 
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
    c.executemany(
        """
        INSERT INTO nflReceivers (
        pfr_player_id,
        player_name,
        career_try,
        career_ranypa,
        career_wowy,
        bcs_rating
        ) VALUES (?,?, ?, ?, ?, ?)""",
        payload,
    )
    conn.commit()
    conn.close()
    return "nflReceivers.db"
