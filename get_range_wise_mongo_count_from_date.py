from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import sys
import argparse

PROPERTY_FILE = "/etc/mongoremodel.properties"
config_dict = {}

def read_property_file() -> tuple[bool, dict]:
    """
    Reads the property file and returns its contents as a dictionary.

    Returns:
        tuple: (success: bool, result: dict)
            - success: True if file was read successfully, False otherwise
            - result: Dictionary containing property file contents or error message
    """
    try:
        global config_dict
        config_dict = {}

        with open(PROPERTY_FILE, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    if '=' in line:
                        key, value = line.split('=', 1)
                        config_dict[key.strip()] = value.strip()
        return True, config_dict
    except Exception as e:
        return False, f"Failed to read property file: {str(e)}"

read_property_file()

def parse_mongo_uri(uri: str) -> dict:
    """
    Converts a MongoDB URI into a dictionary with the specified format.
    If fields are not present, they are set to None.

    Args:
        uri (str): MongoDB URI to parse

    Returns:
        dict: Dictionary containing parsed MongoDB configuration
    """
    try:
        # Initialize with default values
        config = {
            "host": None,
            "port": None,
            "user": None,
            "auth_source": "admin",
            "passwd": None
        }

        # Remove mongodb:// prefix if present
        if uri.startswith("mongodb://"):
            uri = uri[10:]

        # Split into parts
        parts = uri.split("@")

        # Parse authentication if present
        if len(parts) > 1:
            auth_part = parts[0]
            host_part = parts[1]

            # Parse username and password
            auth_parts = auth_part.split(":")
            if len(auth_parts) == 2:
                config["user"] = auth_parts[0]
                config["passwd"] = auth_parts[1]
        else:
            host_part = parts[0]

        # Parse host and port
        host_parts = host_part.split("/")
        host_port = host_parts[0].split(":")

        config["host"] = host_port[0]
        if len(host_port) > 1:
            config["port"] = int(host_port[1])

        # Parse auth source if present
        if len(host_parts) > 1:
            options = host_parts[1].split("&")
            for option in options:
                if option.startswith("authSource="):
                    config["auth_source"] = option[11:]

        return config
    except Exception as e:
        return {
            "host": None,
            "port": None,
            "user": None,
            "auth_source": None,
            "passwd": None
        }

# Parse MongoDB URIs from config
source_mongo_config = parse_mongo_uri(config_dict['src_mongo_uri'])
destination_mongo_config = parse_mongo_uri(config_dict['dst_mongo_uri'])

def get_mongo_client(config):
    uri = f"mongodb://{config['user']}:{config['passwd']}@{config['host']}:{config['port']}/{config['auth_source']}"
    return MongoClient(uri)

def get_count_with_ad_filter(collection_object, start_uid, end_uid, ad):
    """
    Calculates the total count of events (array elements) across all relevant array fields
    for documents within a specified range of uids, considering only those events
    within the arrays where the 'ad' field is greater than 240401.

    Args:
        collection_object: The PyMongo collection object for 'userDetails'.
        start_uid: The starting value of the uid range (inclusive).
        end_uid: The ending value of the uid range (inclusive).

    Returns:
        int: The total count of qualifying events, or None if an error occurs.
    """
    pipeline = [
        {
            "$match": {
                "uid": { "$gte": start_uid, "$lte": end_uid }
            }
        },
        {
            "$project": {
                "_id": 0,
                "eventArrays": {
                    "$filter": {
                        "input": { "$objectToArray": "$$ROOT" },
                        "as": "item",
                        "cond": { "$not": { "$in": ["$$item.k", ["_id", "uid", "a", "l", "ts"]] } }
                    }
                }
            }
        },
        {
            "$unwind": "$eventArrays"
        },
        {
            "$match": {
                "$expr": { "$isArray": "$eventArrays.v" }
            }
        },
        {
            "$project": {
                "validEvents": {
                    "$filter": {
                        "input": "$eventArrays.v",
                        "as": "event",
                        "cond": { "$gt": ["$$event.ad", ad] }
                    }
                }
            }
        },
        {
            "$group": {
                "_id": None,
                "totalValidEvents": { "$sum": { "$size": "$validEvents" } }
            }
        },
        {
            "$project": {
                "_id": 0,
                "totalValidEvents": 1
            }
        }
    ]

    try:
        result = list(collection_object.aggregate(pipeline))
        if result:
            return result[0].get("totalValidEvents", 0)
        else:
            return 0
    except Exception as e:
        print(f"An error occurred during aggregation: {e}")
        return None

def count_documents_in_batches(mongo_config, label, panel_name, coll_name, start_uid, end_uid, batch, ad, verbose):
    try:
        client = get_mongo_client(mongo_config)
        db = client[panel_name]  # Adjust DB name if different
        col = db[coll_name]
        total = 0
        current = start_uid
        while current < end_uid:
            count = get_count_with_ad_filter(col, current, current+batch, ad)
            if count is not None:
                total += count
                if verbose:
                    print(f"[{label}] UIDs {current} - {current + batch - 1}: Count (events with ad > 240401) = {count}")
            current += batch
        print(f"\n[{label}] [panel: {panel_name}] [ev_type: {coll_name}] [uid_range: {start_uid:,} to {end_uid:,}] [total_events_ad_gt_240401: {total}]")

    except ConnectionFailure as e:
        print(f"Failed to connect to {label} MongoDB: {e}")

# Run for source


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process input files and parameters.")
    parser.add_argument("panel_name", help="Name of the panel")
    parser.add_argument("coll_name", help="Name of the collection")
    parser.add_argument("start_uid", type=int, help="Start uid")
    parser.add_argument("end_uid", type=int, help="End uid")
    parser.add_argument("batch", type=int, help="Batch size", default=10000)
    parser.add_argument("ad", type=int, help="Ad", default=240401)
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output")
    args = parser.parse_args()

    panel_name = args.panel_name
    coll_name = args.coll_name
    start_uid = args.start_uid
    end_uid = args.end_uid
    batch = args.batch
    ad = args.ad
    verbose = args.verbose
    
    count_documents_in_batches(source_mongo_config, "Source", panel_name, coll_name, start_uid, end_uid, batch, ad, verbose)

