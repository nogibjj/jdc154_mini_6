from mylib.extract import extract
from mylib.transform_load import load
from mylib.query import read, create, update, delete
import os


def test_extract():
    result = extract()
    assert os.path.exists(result)


def test_load():
    database = load()
    assert database == "nflReceivers.db"


def test_query():
    created = create()
    readed = read()
    updated = update()
    deleted = delete()
    assert created == "Successful insertion into table"
    assert readed == "Success"
    assert updated == "Successfully updated"
    assert deleted == "Successfully deleted"


if __name__ == "__main__":
    test_load()
    test_extract()
    test_query()
