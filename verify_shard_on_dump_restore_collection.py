from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import logging
from datetime import datetime
import sys
from pymongo import MongoClient

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


NUM_OF_SOURCE_TS_COLLETIONS = 6
NUM_OF_DESTINATION_TS_COLLETIONS = 6

TIMESERIES_COLLECTIONS = ["userAttributes", "anonUserAttributes", "disableUserAttributes", "userEvents", "anonUserEvents", "disableUserEvents", "anonEngagementDetails", "anonUserDetails", "disableEngagementDetails", "disableUserDetails", "engagementDetails", "userDetails"]
# TIMESERIES_COLLECTIONS = ["test_1_c1", "test_1_c2", "test_2_c1", "test_2_c2"]

def setup_logger(log_file_path):
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(message)s",
        handlers=[
            logging.FileHandler(log_file_path, mode="a")
        ]
    )


def log_message(level: str, data: dict):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_level = level.upper()
    log_details = " ".join([f"[{key}:{val}]" for key, val in data.items()])
    message = f"{timestamp} {log_level} {log_details}"
    
    if log_level == "DEBUG":
        logging.debug(message)
    elif log_level == "INFO":
        logging.info(message)
    elif log_level == "WARNING":
        logging.warning(message)
    elif log_level == "ERROR":
        logging.error(message)
    elif log_level == "CRITICAL":
        logging.critical(message)
    else:
        logging.info(message)


def get_mongo_connection(host, port, username=None, password=None, auth_source="admin"):
    try:
        uri = f"mongodb://{username}:{password}@{host}:{port}/" if username and password else f"mongodb://{host}:{port}/"
        
        client = MongoClient(uri, serverSelectionTimeoutMS=5000, authSource=auth_source)
        client.admin.command('ping')
        # log_message('INFO', {"msg": "Connected to Mongo"})
        return client
    except ConnectionFailure as e:
        log_message('ERROR', {"msg": "Failed to connect to Mongo", "err": e})
        exit()
    except Exception as e:
        log_message('ERROR', {"msg": "Unexpected error occurred while conneting to mongo", "err": e})
        exit()
    

# NOTE: THIS WILL ONYL WORK FOR NON TIMESERIES COLLECTIONS
def verify_collections_and_shards(panels, source_client, dest_client):
    try:
        for db_name in panels:
            if db_name in ["admin", "local", "config"]:
                continue

            source_db = source_client[db_name]
            dest_db = dest_client[db_name]

            source_collections = source_db.list_collection_names()
            dest_collections = dest_db.list_collection_names()

            # source_collections_len = len(source_collections) - NUM_OF_SOURCE_TS_COLLETIONS
            # dest_collections_len = len(dest_collections) - NUM_OF_DESTINATION_TS_COLLETIONS
            # if source_collections_len != dest_collections_len:
            #     log_message("ERROR", {
            #         "msg": f"Missmatch in number of collections.",
            #         "db": db_name,
            #         "source_collection_length": source_collections_len,
            #         "destination_collection_length": dest_collections_len
            #     })

            for coll in source_collections:
                if coll in TIMESERIES_COLLECTIONS:
                    continue

                # Check collection exists
                if coll not in dest_collections:
                    log_message("ERROR", {
                        "msg": f"Collection {db_name}.{coll} exists in source but not in destination"
                    })
                else:
                    log_message("INFO", {
                        "msg": f"Collection {db_name}.{coll} exists in both source and destination"
                    })

                # Check shard key
                source_shard = source_client["config"].collections.find_one({"_id": f"{db_name}.{coll}"})
                dest_shard = dest_client["config"].collections.find_one({"_id": f"{db_name}.{coll}"})

                if source_shard and source_shard.get("key"):
                    if dest_shard and dest_shard.get("key"):
                        if source_shard["key"] == dest_shard["key"]:
                            log_message("INFO", {
                                "msg": f"Shard key for {db_name}.{coll} matches in both source and destination"
                            })
                        else:
                            log_message("ERROR", {
                                "msg": f"Shard key mismatch for {db_name}.{coll}",
                                "source_key": source_shard["key"],
                                "dest_key": dest_shard["key"]
                            })
                    else:
                        log_message("ERROR", {
                            "msg": f"Shard key missing in destination for {db_name}.{coll}",
                            "expected_key": source_shard["key"]
                        })
                else:
                    # Should not be sharded in destination either
                    if dest_shard and dest_shard.get("key"):
                        log_message("ERROR", {
                            "msg": f"{db_name}.{coll} is not sharded in source but is sharded in destination",
                            "dest_key": dest_shard["key"]
                        })
                    else:
                        log_message("INFO", {
                            "msg": f"{db_name}.{coll} is not sharded in either source or destination"
                        })

    except Exception as e:
        log_message("ERROR", {
            "msg": "Exception occurred during verification",
            "err": str(e)
        })

if __name__ == '__main__':
    if len(sys.argv) == 3:
        panels_file_path = sys.argv[1]
        log_file_path = sys.argv[2]
    else:
        print('usage: python3 create_shard_on_dump_restore_creation.py <panels_file_path> <log_file_path>')
        exit()
    
    setup_logger(log_file_path)

    with open(panels_file_path, "r") as file:
        panels = [line.strip() for line in file if line.strip()]
    
    source_mongo_client = get_mongo_connection(source_mongo_config['host'], source_mongo_config['port'], source_mongo_config['user'], source_mongo_config['passwd'])
    destination_mongo_client = get_mongo_connection(destination_mongo_config['host'], destination_mongo_config['port'], destination_mongo_config['user'], destination_mongo_config['passwd'])

    verify_collections_and_shards(panels, source_mongo_client, destination_mongo_client)
