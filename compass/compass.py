from pymongo import MongoClient
import os
from dotenv import load_dotenv
# Requires the PyMongo package.
# https://api.mongodb.com/python/current
load_dotenv(dotenv_path="D:/NoSQL/MongoDBU/.env")
MONGODB_URI = os.getenv("MONGODB_URI")

client = MongoClient(MONGODB_URI)
result = client['sample_restaurants']['restaurants'].aggregate([
    {
        '$group': {
            '_id': '$cuisine', 
            'top_restaurants': {
                '$topN': {
                    'output': [
                        '$name', '$address.coord'
                    ], 
                    'sortBy': {
                        'stars': 1
                    }, 
                    'n': 5
                }
            }
        }
    }
])