from pymongo import MongoClient

def find_databases_with_user_events(mongo_uri):
    """
    Connects to a MongoDB instance using the provided URI, scans all databases,
    and prints the names of databases that contain a collection named 'userEvents'.

    Args:
        mongo_uri (str): The MongoDB connection URI.

    Returns:
        list: A list of database names that contain the 'userEvents' collection.
    """
    try:
        client = MongoClient(mongo_uri)
        database_names = client.list_database_names()
        databases_with_user_events = []

        for db_name in database_names:
            db = client[db_name]
            collection_names = db.list_collection_names()
            if 'userEvents' in collection_names:
                print(f"Database '{db_name}' has the 'userEvents' collection.")
                databases_with_user_events.append(db_name)

        return databases_with_user_events

    except Exception as e:
        print(f"An error occurred: {e}")
        return []
    finally:
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    mongo_uri = "mongodb://root:icyiguana30@nc-mgin3-cfg2.netcore.in:27015/"  # Replace with your actual MongoDB URI
    databases = find_databases_with_user_events(mongo_uri)
    if databases:
        print("\nDatabases containing 'userEvents':", databases)
    else:
        print("\nNo databases found with the 'userEvents' collection.")
