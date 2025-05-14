from pymongo import MongoClient
import sys

# MongoDB connection details
MONGO_HOST = "nc-tsmgus5-cfg1.netcorein.com"
MONGO_PORT = 27015
MONGO_USERNAME = "root"
MONGO_PASSWORD = "icyiguana30"
MONGO_AUTH_DB = "admin"  # Corrected to "admin"

# Database and collection name
DATABASE_NAME = sys.argv[1]  # Corrected to "smartfrenapn"
COLLECTION_NAME = sys.argv[2]

def get_connection():
    """Establishes and returns a MongoDB connection."""
    client = MongoClient(
        host=MONGO_HOST,
        port=MONGO_PORT,
        username=MONGO_USERNAME,
        password=MONGO_PASSWORD,
        authSource=MONGO_AUTH_DB  # Corrected option name
    )
    return client

def count_user_events(db, start_uid, end_uid, ev_type):
    """Counts user events within a specific UID range and event type."""
    if ev_type == "engagement":
        query = {
            "nc_meta.uid": {"$gte": start_uid, "$lte": end_uid},
            "nc_meta.ev": {"$lte": 10000}
        }
    elif ev_type == "channel":
        query = {
            "nc_meta.uid": {"$gte": start_uid, "$lte": end_uid},
            "nc_meta.ev": {"$gt": 10000}
        }
    else:
        query = {
            "nc_meta.uid": {"$gte": start_uid, "$lte": end_uid},
        }
    
    count = db[COLLECTION_NAME].count_documents(query)
    return count

if __name__ == "__main__":
    client = get_connection()
    db = client[DATABASE_NAME]
    total_count = 0

    start_range = int(sys.argv[3])
    end_range = int(sys.argv[4])
    max_uid = int(sys.argv[4])
    gap = int(sys.argv[5])
    ev_type = sys.argv[6]


    print("Counting user events for UID ranges:")

    while start_range <= max_uid:
        current_count = count_user_events(db, start_range, end_range, ev_type)
        # print(f"UID Range: {start_range:,} to {end_range:,} - Count: {current_count:,}")
        total_count += current_count
        start_range = end_range + 1
        end_range += gap
        if end_range > max_uid:
            end_range = max_uid

    print(f"\n[Destination] [ev_type: {ev_type}] [panel: {DATABASE_NAME}] [uid_range: {start_range:,} to {end_range:,}] [total_count: {total_count}")

    client.close()
