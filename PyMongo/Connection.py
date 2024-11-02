import os
from dotenv import load_dotenv
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

load_dotenv(dotenv_path="D:/NoSQL/MongoDBU/.env")
MONGODB_URI = os.getenv("MONGODB_URI")
# Create a new client and connect to the server
client = MongoClient(MONGODB_URI, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
    for db_name in client.list_database_names():
        print(db_name)
except Exception as e:
    print(e)
client.close()
    
