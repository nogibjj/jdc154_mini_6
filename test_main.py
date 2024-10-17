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
    server_h = os.getenv("SERVER_HOSTNAME")
    access_token = os.getenv("ACCESS_TOKEN")
    # http_path = os.getenv("HTTP_PATH")
    with sql.connect(
        server_hostname=server_h,
        http_path="/sql/1.0/warehouses/2d6f41451e6394c0",
        access_token=access_token,
    ) as connection:
        c = connection.cursor()
        c.execute("SELECT * from jdc_draft_2015")
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
