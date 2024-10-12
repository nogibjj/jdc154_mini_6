"""
ETL-Query script
"""

from mylib.extract import extract
from mylib.transform_load import load
from mylib.query import read, create, update, delete, query2, query3

# Extract
print("Extracting data...")
extract()

# Transform and load
print("Transforming data...")
load()

# Query
print("Querying data...")
read()

# Create
create()

# Update
update()

# Delete
delete()

# Query 2 & 3
query2()
query3()
