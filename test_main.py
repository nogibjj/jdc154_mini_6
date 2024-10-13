from mylib.extract import extract
from mylib.transform_load import load
from mylib.query import query
import os
from databricks import sql
from dotenv import load_dotenv


def test_extract():
    result = extract()
    assert os.path.exists(result)


def test_load():
    load_dotenv()
    host_name = os.getenv("SERVER_HOST")
    token = os.getenv("ACCESS_TOKEN")
    http = os.getenv("HTTP_PATH")
    with sql.connect(
        server_hostname=host_name, http_path=http, access_token=token
    ) as connection:
        c = connection.cursor()
        c.execute("SELECT * from jdc_nflReceivers")
        result = c.fetchall()
        c.close()
    assert result is not None


def test_query():
    queried = query()

    assert queried == "Successfully queried"


if __name__ == "__main__":
    test_load()
    test_extract()
    test_query()
