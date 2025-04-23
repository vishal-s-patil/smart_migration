from pymongo import MongoClient
import sys

# local 
# mogno_config = {
#     "username": "admin",
#     "password": "admin",
#     "auth_source": "admin",
#     "host": "localhost",
#     "port": 27017
# }

# mongo7

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
destination_mongo_config = parse_mongo_uri(config_dict['dst_mongo_uri'])

mogno_config = {
    "username": destination_mongo_config['user'],
    "password": destination_mongo_config['passwd'],
    "auth_source": destination_mongo_config['auth_source'],
    "host": destination_mongo_config['host'],
    "port": destination_mongo_config['port']
}

expected_timeseries_info = {
    "timeField": "evt",
    "metaField": "nc_meta",
    "granularity": "hours",
}

collection_info = {
    "userAttributes": {
        "indexes": [
            ({"uid":1},{"unique":True}),
            ({"l.lid":1},{"background":True}),
            ({ "uid": "hashed" })
        ],
        "shard_keys": { "uid": "hashed"},
        "is_timeseries": False
    },
    "anonUserAttributes": {
        "indexes": [
            ({"uid":1},{"unique":True}),
            ({"l.lid":1},{"background":True}),
            ({ "uid": "hashed" })
        ],
        "shard_keys": { "uid": "hashed"},
        "is_timeseries": False
    },
    "disableUserAttributes": {
        "indexes": [
            ({"uid":1},{"unique":True}),
            ({"l.lid":1},{"background":True}),
            ({ "uid": "hashed" })
        ],
        "shard_keys": { "uid": "hashed"},
        "is_timeseries": False
    },
    "userEvents": {
        "indexes": [
            ({ "nc_meta": 1, "evt": 1 }, { "name": 'nc_meta_1_evt_1' }),
            ({ "nc_meta.uid": 1, "nc_meta.ev": 1, "evt": 1 }, { "name": "nc_meta.uid_1_nc_meta.ev_1_evt_1" }),
            ({ "uid": 1, "ev": 1, "evt": 1 },{ "name": "uid_1_ev_1_evt_1" }),
            ({ "nc_meta.uid": "hashed" })
        ],
        "shard_keys": { "meta.uid": "hashed" },
        "is_timeseries": True
    },
    "anonUserEvents": {
        "indexes": [
            ({ "nc_meta": 1, "evt": 1 }, { "name": 'nc_meta_1_evt_1' }),
            ({ "nc_meta.uid": 1, "nc_meta.ev": 1, "evt": 1 }, { "name": "nc_meta.uid_1_nc_meta.ev_1_evt_1" } ),
            ({ "uid": 1, "ev": 1, "evt": 1 },{ "name": "uid_1_ev_1_evt_1" }),
            ({ "nc_meta.uid": "hashed" })
        ],
        "shard_keys": { "meta.uid": "hashed" },
        "is_timeseries": True
    },
    "disableUserEvents": {
        "indexes": [
            ({ "nc_meta": 1, "evt": 1 }, { "name": 'nc_meta_1_evt_1' }),
            ({ "nc_meta.uid": 1, "nc_meta.ev": 1, "evt": 1 }, { "name": "nc_meta.uid_1_nc_meta.ev_1_evt_1" }),
            ({ "uid": 1, "ev": 1, "evt": 1 },{ "name": "uid_1_ev_1_evt_1" }),
            ({ "nc_meta.uid": "hashed" })
        ],
        "shard_keys": { "meta.uid": "hashed" },
        "is_timeseries": True
    },
    # "anonFrequencyDetails": {
    #     "indexes": [
    #         ({ "uid": 1, "period": 1 }, { "name": "uid_1_period_1", "background":True, "unique":True }),
    #         ({ "expireAt": 1 }, { "name": "expireAt_1", "background":True, "expireAfterSeconds": 0 })
    #     ],
    #     "shard_keys": { "uid": 1 },
    #     "is_timeseries": False
    # },
    # "anonUserGuidMaster": {
    #     "indexes": [
    #         ({ "uid": 1 }, { "name": "uid_1", "background":True }),
    #         ({ "guid": "hashed" }, { "name": "guid_hashed", "background":True }),
    #         ({ "guid": 1, "uid": 1 }, { "name": "guid_1_uid_1", "background":True, "unique":True })
    #     ],
    #     "shard_keys": { "guid": "hashed" },
    #     "is_timeseries": False
    # },
    # "frequencyDetails": {
    #     "indexes": [
    #         ({ "uid": 1, "period": 1 }, { "name": "uid_1_period_1", "background":True, "unique":True }),
    #         ({ "expireAt": 1 }, { "name": "expireAt_1", "background":True, "expireAfterSeconds": 0 })
    #     ],
    #     "shard_keys": { "uid": 1 },
    #     "is_timeseries": False
    # },
    # "segmentUsers": {
    #     "indexes": [
    #         ({ "_id": "hashed" }, { "name": "_id_hashed", "background":True }),
    #         ({ "segId": -1 }, { "name": "segId_-1", "background":True })
    #     ],
    #     "shard_keys": { "_id": "hashed" },
    #     "is_timeseries": False
    # },
    # "userGuidMaster": {
    #     "indexes": [
    #         ({ "uid": 1 }, { "name": "uid_1", "background":True }),
    #         ({ "guid": "hashed" }, { "name": "guid_hashed", "background":True }),
    #         ({ "guid": 1, "uid": 1 }, { "name": "guid_1_uid_1", "background":True, "unique":True })
    #     ],
    #     "shard_keys": { "guid": "hashed" },
    #     "is_timeseries": False
    # },
}


def create_indexes(client, db_name, collection_name, index_list):
    try:
        db = client[db_name]
        collection = db[collection_name]
    except Exception as e:
        print(f"Error while getting db/collection: {e}")
        exit()
    
    cnt = 0
    for index in index_list:
        try:
            if isinstance(index, tuple):
                keys, options = index
            else:
                keys, options = index, {}
        except Exception as e:
            print(f"Error while parsing index: {e}")
            exit()

        try:
            collection.create_index(list(keys.items()), **options)
            cnt+=1
        except Exception as e:
            print(f"Error while creating index: {e}")
            exit()

    print(f"[Event:create_index] [DB:{db_name}] [Coll:{collection_name}] [Count:{cnt}]")


def check_indexes(client, db_name, collection_name, index_list):
    try:
        db = client[db_name]
        collection = db[collection_name]
    except Exception as e:
        print(f"Error while getting db/collection: {e}")
        exit()

    try:
        existing_indexes = collection.index_information()
    except Exception as e:
        print(f"Error while getting existing indexes: {e}")
        exit()

    for index in index_list:
        try:
            if isinstance(index, tuple):
                keys, options = index
            else:
                keys, options = index, {}
        except Exception as e:
            print(f"Error while parsing index: {e}")
            exit()
        
        index_keys = sorted(keys.items())  
        expected_name = options.get("name", "_".join([f"{k}_{v}" for k, v in index_keys]))


        if expected_name in existing_indexes:
            existing_keys = sorted(existing_indexes[expected_name]["key"])

            existing_options = {
                k: v for k, v in existing_indexes[expected_name].items() if k not in ["v", "key"]
            }
            provided_options = {k: v for k, v in options.items() if k != "name"}  # Ignore name
            
            if existing_keys == index_keys and existing_options == provided_options:
                print(f"[Info] [Event:check_indexes] [DB:{db_name}] [Collection:{collection_name}] [Index:{expected_name}] [Msg:exists with matching keys and options]")
            else:
                if existing_keys != index_keys:
                    print(f"[Error] [Event:check_indexes] [DB:{db_name}] [Collection:{collection_name}] [Index:{expected_name}] [Msg:exists but has difference in keys] [Expected:{index_keys}] [Found:{existing_keys}]")
                if existing_options != provided_options:
                    print(f"[Error] [Event:check_indexes] [DB:{db_name}] [Collection:{collection_name}] [Index:{expected_name}] [Msg:exists but has difference in options] [Expected:{provided_options}] [Found:{existing_options}]")
        else:
            print(f"[Error] [Event:check_indexes] [DB:{db_name}] [Collection:{collection_name}] [Index:{expected_name}] [Msg:does not exists]")


def check_collections(client, db_name, expected_collection_count, expected_collections):
    try:
        db = client[db_name]
        existing_collections = sorted(db.list_collection_names())
        expected_collections = sorted(expected_collections)

        flag = False
        for expected_collection in expected_collections:
            if expected_collection not in existing_collections:
                flag = True
                print(f"[Error] [Event:check_collections] [Msg:collection not found] [DB:{db_name}] [Collection:{expected_collection}]")
                continue
        if not flag:
            print(f"[Info] [Event:check_collections] [DB:{db_name}] [Msg:collection names matched]")
            return True
        
    except Exception as e:
        print(f"Error while checking collection count: {e}")
        return False 


def check_sharding(client, db_name, collection_name, expected_shard_key, is_timeseries):
    config_db = client.config
    if is_timeseries:
        namespace = f"{db_name}.system.buckets.{collection_name}"
    else:
        namespace = f"{db_name}.{collection_name}"
    
    collection_info = config_db.collections.find_one({"_id": namespace})

    if not collection_info:
        print(f"[Error] [Event:check_sharding] [DB:{db_name}] [Collection:{collection_name}] [Msg:sharding does not exists]")
        return False
    
    existing_shard_key = collection_info["key"]
    
    if existing_shard_key == expected_shard_key:
        print(f"[Info] [Event:check_sharding] [DB:{db_name}] [Collection:{collection_name}] [Msg:shard key matches]")
        return True
    else:
        print(f"[Error] [Event:check_sharding] [DB:{db_name}] [Collection:{collection_name}] [Msg:shard key mismatch] [Expected:{expected_shard_key}] [Found:{existing_shard_key}]")
        return False


def get_timeseries_info(client, db_name, collection_name):
    db = client[db_name]

    collection_info = db.command("listCollections", filter={"name": collection_name})
    
    if not collection_info["cursor"]["firstBatch"]:
        return

    info = collection_info["cursor"]["firstBatch"][0]
    
    if info["type"] != "timeseries":
        print(f"[Error] [Event:check_timeseries_info] [DB:{db_name}] [Collection:{collection_name}] [Msg:not a time-series collection]")
        return
    
    if info['options']['timeseries']['timeField'] != expected_timeseries_info["timeField"] or info['options']['timeseries'].get('metaField', 'None') != expected_timeseries_info["metaField"] or info['options']['timeseries']['granularity'] != expected_timeseries_info["granularity"]:
        existing_timeseries_info = {
            "timeField": info['options']['timeseries']['timeField'],
            "metaField": info['options']['timeseries'].get('metaField', 'None'),
            "granularity": info['options']['timeseries']['granularity'],
        }
        print(f"[Error] [Event:check_timeseries_info] [DB:{db_name}] [Collection:{collection_name}] [Msg:different timeseries configuration] [Expected:{expected_timeseries_info}] [Exisising:{existing_timeseries_info}]")
        return
    

if __name__ == "__main__":
    if len(sys.argv) == 2:
        panels_file_path = sys.argv[1]
    else:
        print("Usage: python3 ts_mongo_ind_validation.py <panels_file_path>")
        exit()

    try:
        client = MongoClient(f"mongodb://{mogno_config['username']}:{mogno_config['password']}@{mogno_config['host']}:{mogno_config['port']}/?authSource={mogno_config['auth_source']}")  
    except Exception as e:
        print(f"Error while connecting to mongo: {e}")
        exit()
    
    try:
        with open(panels_file_path, "r") as file:
            panels = [line.strip() for line in file if line.strip()]
    except Exception as e:
        print(f"Error while reading panels file: {e}")
        exit()

    expected_collection_count = len(collection_info)

    for panel in panels:
        check_collections(client, panel, expected_collection_count, collection_info.keys())
        
        for collection_name, info in collection_info.items():
            check_indexes(client, panel, collection_name, info["indexes"])
        
        for collection_name, info in collection_info.items():
            check_sharding(client, panel, collection_name, info["shard_keys"], info['is_timeseries'])

        for collection_name, info in collection_info.items():
            if info.get("is_timeseries", False):
                get_timeseries_info(client, panel, collection_name)
