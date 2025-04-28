import redis
import json
import logging
import requests
import psutil
from datetime import datetime
from confluent_kafka import Consumer, TopicPartition
import sys
import pymongo
import os

# Load properties from file
def load_properties(filepath):
    properties = {}
    try:
        with open(filepath, "r") as file:
            for line in file:
                if "=" in line and not line.startswith("#"):
                    key, value = line.strip().split("=", 1)
                    properties[key.strip()] = value.strip()
    except FileNotFoundError:
        raise
    return properties

# Load properties
PROPERTIES_FILE = "/etc/mongoremodel.properties"
props = load_properties(PROPERTIES_FILE)

# Extract configurations
KAFKA_BROKER = props.get("kafka_bootstrap_servers", "localhost:9092")
REDIS_HOST = props.get("redis_uri", "localhost")
REDIS_PORT = int(props.get("redis_port", 6379))
SLACK_WEBHOOK_URL = props.get("collection_creation_alert_slack_url", "")
ENABLE_LOGGING = props.get("enable_monitoring_script_debug_logging", "false").lower() == "true"
ENV = props.get("env", "")
SOURCE_MONGO_URI = props.get("src_mongo_uri", "")
TARGET_MONGO_URI = props.get("dst_mongo_uri", "")

# Setup logging
LOG_FILE = "/var/log/apps/mongodataremodel/check_wrong_collection_creation.log"
logging.basicConfig(filename=LOG_FILE, level=logging.DEBUG if ENABLE_LOGGING else logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

mongo_7_check_list = ["anonEngagementDetails", "anonUserDetails", "disableEngagementDetails", "disableUserDetails", "engagementDetails", "userDetails"]

mongo_5_check_list = ["anonUserAttributes", "anonUserEvents", "disableUserAttributes", "disableUserEvents", "userAttributes", "userEvents"]

def get_mongo_client(uri: str) -> pymongo.MongoClient:
    """
    Creates and returns a MongoDB client using the provided URI.
    
    Args:
        uri (str): MongoDB connection URI
        
    Returns:
        pymongo.MongoClient: MongoDB client instance
        
    Raises:
        Exception: If connection fails
    """
    try:
        client = pymongo.MongoClient(uri)
        # Test the connection
        client.admin.command('ping')
        return client
    except Exception as e:
        raise Exception(f"Failed to connect to MongoDB: {str(e)}")

def check_collections_in_databases(client: pymongo.MongoClient, collection_list: list) -> dict:
    """
    Checks if any database has the specified collections.
    
    Args:
        client (pymongo.MongoClient): MongoDB client instance
        collection_list (list): List of collection names to check
        
    Returns:
        dict: Dictionary containing results with database names as keys and 
              list of found collections as values
    """
    results = {}
    
    # Get list of all databases
    database_list = client.list_database_names()
    
    # Skip system databases
    system_dbs = ['admin', 'local', 'config']
    database_list = [db for db in database_list if db not in system_dbs]
    
    for db_name in database_list:
        db = client[db_name]
        found_collections = []
        
        existing_collections = db.list_collection_names()

        for collection_name in collection_list:
            if collection_name in existing_collections:
                found_collections.append(collection_name)
        
        if found_collections:
            results[db_name] = found_collections
    
    return results

def send_slack_alert(mongo_version, message):
    if not SLACK_WEBHOOK_URL:
        logger.warning("Slack webhook URL is not configured. Alert not sent.")    
        return
    
    payload = {
        "text": (
            f"<!channel> \n"
            f"📍 *Env:* `{ENV}` \n"
            f"📍 *Mongo Version:* `{mongo_version}` \n"
            f"📍 *Message:* `{message}` \n"
        )
    }
    logger.debug(f"Sending Slack alert: {json.dumps(payload)}")
    response = requests.post(SLACK_WEBHOOK_URL, data=json.dumps(payload), headers={"Content-Type": "application/json"})


def main():       
    try:
        # Get MongoDB client
        src_client = get_mongo_client(SOURCE_MONGO_URI)
        dst_client = get_mongo_client(TARGET_MONGO_URI)
        
        # Check collections across all databases
        src_results = check_collections_in_databases(src_client, mongo_5_check_list)
        dst_results = check_collections_in_databases(dst_client, mongo_7_check_list)
        
        # if results is not empty, send slack alert
        if src_results:
            send_slack_alert("Mongo 5", "Found collections in the following databases:\n" + str(src_results))
        if dst_results:
            send_slack_alert("Mongo 7", "Found collections in the following databases:\n" + str(dst_results))

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)
    finally:
        # Close the MongoDB client
        if 'src_client' in locals():
            src_client.close()
        if 'dst_client' in locals():
            dst_client.close()

if __name__ == "__main__":
    main()
