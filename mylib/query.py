"""Query the database"""

import os
from databricks import sql
from dotenv import load_dotenv

complex_query = """
    WITH av_super AS (
    SELECT Position, AVG(Superstar) as average
    from jdc_draft_2015
    GROUP BY Position
    )

    SELECT p.Player, p.NameID, p.Superstar, p.Position, a.average
    FROM jdc_draft_2015 p 
    JOIN av_super a 
    ON p.Position = a.Position
    WHERE Superstar > a.average
    ORDER BY Position, Superstar DESC
"""


def query():
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
        c.execute(complex_query)
        result = c.fetchall()
        print("Query Output: \n")
        print(result)
        c.close()
    return "Successfully queried"
