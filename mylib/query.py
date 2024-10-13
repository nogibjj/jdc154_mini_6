"""Query the database"""

import os
from databricks import sql
from dotenv import load_dotenv

complex_query = """
    WITH avg AS (
    SELECT 
    )
"""


def query():
    load_dotenv()
    host_name = os.getenv("SERVER_HOST")
    token = os.getenv("ACCESS_TOKEN")
    http = os.getenv("HTTP_PATH")
    with sql.connect(
        server_hostname=host_name, http_path=http, access_token=token
    ) as connection:
        c = connection.cursor()
        c.execute(complex_query)
        c.close()
    print("Successfully transformed and loaded to Databricks")
    return "Successfully queried"
