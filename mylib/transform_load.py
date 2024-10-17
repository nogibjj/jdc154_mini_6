"""
Transforms and Loads data into the local SQLite3 database
"""

from dotenv import load_dotenv
from databricks import sql
import pandas as pd
import os


def load(dataset="data/nba-draft-2015.csv"):
    """Transforms and Loads data into the local databricks database"""
    df = pd.read_csv(dataset, delimiter=",", skiprows=1)
    load_dotenv()
    server_h = os.getenv("SERVER_HOSTNAME")
    access_token = os.getenv("ACCESS_TOKEN")
    # http_path = os.getenv("HTTP_PATH")
    with sql.connect(
        server_hostname=server_h,
        http_path="/sql/1.0/warehouses/2d6f41451e6394c0",
        access_token=access_token,
    ) as connection:
        c = connection.cursor()
        c.execute("SHOW TABLES FROM default LIKE 'jdc_draft_2015*'")
        result = c.fetchall()

        if not result:
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS jdc_draft_2015 (
                    Player STRING,
                    Position STRING,
                    NameID STRING,
                    DraftYear INT,
                    ProjectedSPM DOUBLE,
                    Superstar DOUBLE,
                    Starter DOUBLE,
                    RolePlayer DOUBLE,
                    Bust DOUBLE
                )
            """
            )
        c.execute("SELECT * from jdc_draft_2015")
        result = c.fetchall()
        if not result:

            for _, row in df.iterrows():
                convert = tuple(row)
                c.execute(f"INSERT INTO jdc_draft_2015 VALUES {convert}")
        c.close()
    print("successfully loaded data into Databricks")
    return "success"


if __name__ == "__main__":
    load()
