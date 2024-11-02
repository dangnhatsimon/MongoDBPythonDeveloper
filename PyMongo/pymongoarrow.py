import pymongoarrow
from pymongo import MongoClient
import pandas
import pyarrow
import numpy
import pprint
from datetime import datetime
from pymongoarrow.monkey import patch_all
from pymongoarrow.api import Schema
from pymongoarrow.api import write
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path="D:/NoSQL/MongoDBU/.env")
MONGODB_URI = os.getenv("MONGODB_URI")

client = MongoClient(MONGODB_URI)

# List all database names
client.list_database_names()

db = client.newDB
col = db.newCol

# Create a list of documents
data = [
    {"_id": 1, "measure": 43, "status": "active", "installed_on": datetime(2022, 1, 8, 3, 43, 12)},
    {"_id": 2, "measure": 32, "status": "active", "installed_on": datetime(2022, 2, 2, 11, 43, 27)},
    {"_id": 3, "measure": 62, "status": "inactive", "installed_on": datetime(2022, 3, 12, 3, 53, 12)},
    {"_id": 4, "measure": 59, "status": "active", "installed_on": datetime(2022, 4, 8, 3, 22, 45)}
]

# insert data into our database
col.insert_many(data)


# print data
for doc in col.find({}):
    pprint.pprint(doc)

# Patch pymongo in place
patch_all()

df = col.find_pandas_all(
    {
        "measure": {"$gt": 40}
    }
)
print(df)

# Define schema of data
schema = Schema(
    {
        "_id": int,
        "measure": float,
        "status": str,
        "installed_on": datetime
    }
)


# transfer MongoDB data into numpy array
npa = col.find_numpy_all(
    {
        "measure": {"$gt": 40}
    },
    schema=Schema
)
print(npa)

# transfer MongoDB data into arrow table
arrow_table = col.find_arrow_all({})
print(arrow_table)


# PyMongoArrow Aggregate operations
df_agg = col.aggregate_pandas_all(
    [
        {
            "$match": {
                "measure": {
                    "$gt": 40
                }
            }
        }
    ],
    schema=Schema
)
print(df_agg)


# Writing data back to MongoDB
write(db.pandas_data, df)

# print data
for doc in db.pandas_data.find({}):
    pprint.pprint(doc)


# write the numpy array data back to MongoDB
write(db.numpy_data, npa)

# print data
for doc in db.numpy_data.find({}):
    pprint.pprint(doc)


# write the arrow table data back to MongoDB
write(db.arrow_data, arrow_table)

# print data
for doc in db.arrow_data.find({}):
    pprint.pprint(doc)

