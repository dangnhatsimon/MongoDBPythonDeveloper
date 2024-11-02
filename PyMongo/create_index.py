from pymongo.mongo_client import MongoClient
from pymongo.operations import SearchIndexModel

def create_index():
    # Connect to your Atlas deployment
    uri = "<connectionString>"
    client = MongoClient(uri)

    # Access your database and collection
    database = client["<databaseName>"]
    collection = database["<collectionName>"]

    # Create your index model, then create the search index
    search_index_model = SearchIndexModel(
        definition={
            "mappings": {
                "dynamic": True
            },
        },
        name="<indexName>",
    )

    result = collection.create_search_index(model=search_index_model)
    print(result)


from pymongo.mongo_client import MongoClient
from pymongo.operations import SearchIndexModel

def create_indexes():
    # Connect to your Atlas deployment
    uri = "<connectionString>"
    client = MongoClient(uri)

    # Access your database and collection
    database = client["<databaseName>"]
    collection = database["<collectionName>"]

    # Create your index models and add them to an array
    first_model = SearchIndexModel(
        definition={
            # Add search index definition fields here
        },
        name="<firstIndexName>",
    )

    second_model = SearchIndexModel(
        definition={
            # Add search index definition fields here
        },
        name="<secondIndexName>",
    )

    idx_models = [first_model, second_model]

    # Create the search indexes
    result = collection.create_search_indexes(models=idx_models)
    print(result)