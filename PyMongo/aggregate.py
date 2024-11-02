import os
import pprint

from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv(dotenv_path="D:/NoSQL/MongoDBU/.env")
MONGODB_URI = os.getenv("MONGODB_URI")

# Connect to MongoDB cluster with MongoClient
client = MongoClient(MONGODB_URI)

# Get reference to 'bank' database
db = client.bank

# Get reference to 'accounts' collection
account_collection = db.accounts

# Calculate the average balance of checking and savings accounts with balances of less than $1000.
# Select accounts with balances of less than $1000.
select_by_balance = {
    "$match": {
        "balance": {
            "$lt": 1000
        }
    }
}

# Separate documents by account type and calculate the average balance for each account type.
seperate_by_account_calculate_avg_balance = {
    "$group": {
        "_id": "$account_type",
        "avg_balance": {
            "$avg": "$balance"
        }
    }
}

# Create an aggegation pipeline using 'stage_match_balance' and 'stage_group_account_type'.
pipeline = [
    select_by_balance,
    seperate_by_account_calculate_avg_balance
]

results = account_collection.aggregate(pipeline=pipeline)
print()
print(
    "Average balance of checking and savings accounts with balances of less than $1000:", "\n"
)
for item in results:
    pprint.pprint(item)


# Return the account type, original balance, and balance converted to Great British Pounds (GBP)
# of all checking accounts with an original balance of greater than $1,500 US dollars, in order from highest original balance to lowest.
# To calculate the balance in GBP, divide the original balance by the conversion rate
conversion_rate_usd_to_gbp = 1.3

# Select checking accounts with balances of more than $1,500.
select_accounts = {
    "$match": {
        "account_type": "checking",
        "balance": {
            "$gt": 1500
        }
    }
}

# Organize documents in order from highest balance to lowest.
organize_by_original_balance = {
    "$sort": {
        "balance": -1
    }
}

# Return only the account type & balance fields, plus a new field containing balance in Great British Pounds (GBP).
return_specified_fields = {
    "$project": {
        "account_type": 1,
        "balance": 1,
        "gbd_balance": {
            "$divide": [
                "$balance", conversion_rate_usd_to_gbp
            ]
        },
        "_id": 0
    }
}

# Create an aggegation pipeline containing the four stages created above
pipeline = [
    select_accounts,
    organize_by_original_balance,
    return_specified_fields
]

results = account_collection.aggregate(pipeline=pipeline)
print(
    "Account type, original balance and balance in GDP of checking accounts with original balance greater than $1,500,"
    "in order from highest original balance to lowest: ", "\n"
)
for item in results:
    pprint.pprint(item)

client.close()
