from flask import Flask, jsonify, request
import redis
import subprocess
import os
import csv
import re
from dotenv import load_dotenv
from langchain.agents import create_react_agent, AgentExecutor
from langchain import hub
from langchain.prompts import PromptTemplate
from langchain.memory import ConversationBufferMemory
from langchain.tools import Tool
from langchain_google_genai import ChatGoogleGenerativeAI
from kafka.admin import KafkaAdminClient
from health_check_module import health_check
import shutil
from datetime import datetime
import time
import json
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
import inspect

load_dotenv()

app = Flask(__name__)
# BASE_DIR = "/home/mongodb/migration_orchestration"
BASE_DIR = "/home/mongodb/smart_migration"
LOG_PATH = BASE_DIR + "/logs/smart_migration.log"
PANELS_CID_CSV_FILE_PATH = BASE_DIR + "/panels_cid.csv"
REDIS_BACKUP_DIR = BASE_DIR + "/redis_backup"
HEALTH_CHECK_LOG = BASE_DIR + "/logs/health_check.log"
PANELS_FILE_PATH = BASE_DIR + "/panels.txt"     
PROPERTY_FILE = "/etc/mongoremodel.properties"
BASE_MIGRATION_LOG_DIR = "/var/log/apps/mongodataremodel"
TOPIC_CREATION_LOG = BASE_DIR + "/logs/create_topics.log"
TOPIC_VALIDATION_LOG = BASE_DIR + "/logs/validate_topics.log"
TS_DBS_CREATION_SCRIPT_PATH = BASE_DIR + '/create_db/createdb_timeseries.py'
TS_COLLECTION_CREATION_LOG = BASE_DIR + "/logs/ts_collection_creation.log"
TS_COLLECTION_VALIDATION_LOG = BASE_DIR + "/logs/ts_index_validation.log"
TS_DB_CREATION_LOG = BASE_DIR + "/logs/ts_db_creation.log"
TS_DB_CREATION_LOG = BASE_DIR + "/logs/ts_db_creation.log"
VALIDATION_OF_NON_EXISTANCE_OF_DBS_LOG = BASE_DIR + "/logs/validation_of_non_existence_of_dbs.log"
RUN_PRODUCER_LOG = BASE_DIR + "/logs/run_producer.log"
RUN_CONSUMER_LOG = BASE_DIR + "/logs/run_consumer.log"
KILL_CONSUMER_LOG = BASE_DIR + "/logs/kill_consumer.log"
NUM_PARTITIONS = 10

SLACK_URL = os.getenv("SLACK_URL")
llm = ChatGoogleGenerativeAI(model="gemini-2.0-flash-001", google_api_key=os.getenv("GOOGLE_API_KEY"))

config_dict = {}


def read_property_file(*args, **kwargs) -> tuple[bool, dict]:
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
redis_client = redis.Redis(host=config_dict['redis_uri'], port=config_dict['redis_port'], db=0)

def process_panel(panel_name, cid):
    """Runs the database creation script for a given panel and CID and returns the log string."""
    db_name = f"ts_{panel_name}_{cid}"  # Assuming a naming convention for the DB
    print(panel_name, cid)
    command = [
        "/usr/local/bin/python2.7",
        TS_DBS_CREATION_SCRIPT_PATH,
        panel_name,
        str(cid)
    ]
    result = subprocess.run(command, capture_output=True, text=True)
    output = result.stdout.strip().lower()
    log_string = ""

    if "database created sucessfully" in output:
        log_string = f"[Info] [DB:{db_name}] [msg:created]"
    else:
        error_message = result.stderr.strip()
        log_string = f"[Error] [DB:{db_name}] [msg:failed] [err:{error_message}]"
    return log_string

def create_ts_dbs_collections(*args, **kwargs):
    """
    reads the csv containing the panels and cids and creates the time series databases and collections
    """
    with open(PANELS_CID_CSV_FILE_PATH, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        logs = []
        for row in reader:
            panel_name = row.get('panel')
            cid = int(row.get('cid'))
            if panel_name and cid:
                log_entry = process_panel(panel_name, cid)
                logs.append(log_entry)
            else:
                logs.append(f"[Error] [DB:N/A] [msg:Skipped row due to missing 'panel' or 'cid': {row}]")

    with open(TS_COLLECTION_CREATION_LOG, 'w') as logfile:
        for log in logs:
            logfile.write(log + '\n')

def start_redis(*args, **kwargs):
    """
    Starts the Redis server using systemctl.
    
    Args:
        *args: Variable length argument list
        **kwargs: Arbitrary keyword arguments
    
    Returns:
        tuple: (success: bool, message: str)
            - success: True if Redis started successfully, False otherwise
            - message: Status message describing the result
    """
    try:
        command = ['systemctl', 'start', 'redis']
        print(f"Starting Redis server with command: {' '.join(command)}")
        result = subprocess.run(command, capture_output=True, text=True)
        
        if result.returncode == 0:
            print("Redis server started successfully")
            return True, "Redis server started successfully"
        else:
            error_msg = f"Failed to start Redis: {result.stderr}"
            print(error_msg)
            return False, error_msg
            
    except Exception as e:
        error_msg = f"Failed to start Redis: {str(e)}"
        print(error_msg)
        return False, error_msg

def stop_redis(*args, **kwargs):
    """
    Stops the Redis server using systemctl.
    
    Args:
        *args: Variable length argument list
        **kwargs: Arbitrary keyword arguments
    
    Returns:
        tuple: (success: bool, message: str)
            - success: True if Redis stopped successfully, False otherwise
            - message: Status message describing the result
    """
    try:
        # subprocess.run(['systemctl', 'stop', 'redis'])
        return True, "Redis server stopped successfully"
    except Exception as e:
        return False, f"Failed to stop Redis: {str(e)}"

def delete_keys_by_pattern(pattern, *args, **kwargs):
    """
    Deletes all Redis keys matching the given pattern.
    
    Args:
        pattern (str): Pattern to match keys against (e.g., "producer*")
        *args: Variable length argument list
        **kwargs: Arbitrary keyword arguments
    
    Returns:
        tuple: (success: bool, message: str)
            - success: True if keys were deleted successfully, False otherwise
            - message: Status message describing the result
    """
    try:
        keys = redis_client.keys(pattern)
        if keys:
            pass
            # redis_client.delete(*keys)
        return True, f"Deleted keys matching pattern: {pattern}"
    except Exception as e:
        return False, f"Failed to delete keys: {str(e)}"

def delete_producer_keys(*args, **kwargs):
    """
    Deletes all Redis keys starting with 'producer'.
    
    Args:
        *args: Variable length argument list
        **kwargs: Arbitrary keyword arguments
    
    Returns:
        tuple: (success: bool, message: str)
            - success: True if keys were deleted successfully, False otherwise
            - message: Status message describing the result
    """
    return delete_keys_by_pattern("producer*")

def delete_consumer_keys(*args, **kwargs):
    """
    Deletes all Redis keys starting with 'consumer'.
    
    Args:
        *args: Variable length argument list
        **kwargs: Arbitrary keyword arguments
    
    Returns:
        tuple: (success: bool, message: str)
            - success: True if keys were deleted successfully, False otherwise
            - message: Status message describing the result
    """
    return delete_keys_by_pattern("consumer*")

def delete_all_keys(*args, **kwargs):
    """
    Deletes all keys in the current Redis database.
    
    Args:
        *args: Variable length argument list
        **kwargs: Arbitrary keyword arguments
    
    Returns:
        tuple: (success: bool, message: str)
            - success: True if all keys were deleted successfully, False otherwise
            - message: Status message describing the result
    """
    try:
        redis_client.flushdb()
        return True, "All keys deleted successfully"
    except Exception as e:
        return False, f"Failed to delete all keys: {str(e)}"

def delete_specific_key(key, *args, **kwargs):
    """
    Deletes a specific key from Redis.
    
    Args:
        key (str): The key to delete
        *args: Variable length argument list
        **kwargs: Arbitrary keyword arguments
    
    Returns:
        tuple: (success: bool, message: str)
            - success: True if key was deleted successfully, False otherwise
            - message: Status message describing the result
    """
    try:
        if redis_client.delete(key):
            return True, f"Key '{key}' deleted successfully"
        return False, f"Key '{key}' not found"
    except Exception as e:
        return False, f"Failed to delete key: {str(e)}"

def get_methods_count_form_redis(*args, **kwargs):
    """
    Gets the count of redis keys starting with 'read' and 'write'
    
    Args:
        *args: Variable length argument list
        **kwargs: Arbitrary keyword arguments
    
    Returns:
        tuple: (success: bool, result: dict)
            - success: True if operation was successful, False otherwise
            - result: Dictionary containing read_panels, write_panels, and total_panels counts
    """
    try:
        read_keys = len(redis_client.keys("read*"))
        write_keys = len(redis_client.keys("write*"))
        return True, {
            "read_panels": read_keys,
            "write_panels": write_keys,
            "total_panels": read_keys + write_keys
        }
    except Exception as e:
        return False, f"Failed to get panel count: {str(e)}"

def get_total_keys(*args, **kwargs):
    """
    Gets the total number of keys in Redis.
    
    Args:
        *args: Variable length argument list
        **kwargs: Arbitrary keyword arguments
    
    Returns:
        tuple: (success: bool, result: dict)
            - success: True if operation was successful, False otherwise
            - result: Dictionary containing total_keys count
    """
    try:
        total_keys = len(redis_client.keys("*"))
        return True, {"total_keys": total_keys}
    except Exception as e:
        return False, f"Failed to get total keys: {str(e)}"

def get_panels_length(*args, **kwargs):
    """
    Gets the length of all queues starting with 'read' and 'write'.
    
    Args:
        *args: Variable length argument list
        **kwargs: Arbitrary keyword arguments
    
    Returns:
        tuple: (success: bool, result: dict)
            - success: True if operation was successful, False otherwise
            - result: Dictionary containing read_queues and write_queues with their lengths
    """
    try:
        # Get all keys starting with read and write
        read_keys = [key.decode() for key in redis_client.keys("read*")]
        write_keys = [key.decode() for key in redis_client.keys("write*")]
        
        # Get length of each queue using traditional for loops
        read_queues = {}
        for key in read_keys:
            read_queues[key] = redis_client.llen(key)
            
        write_queues = {}
        for key in write_keys:
            write_queues[key] = redis_client.llen(key)
        
        return True, {
            "read_queues": read_queues,
            "write_queues": write_queues
        }
    except Exception as e:
        return False, f"Failed to get queue lengths: {str(e)}"

def check_redis_status(*args, **kwargs):
    """
    Checks if Redis server is running.
    
    Args:
        *args: Variable length argument list
        **kwargs: Arbitrary keyword arguments
    
    Returns:
        tuple: (success: bool, result: dict)
            - success: True if operation was successful, False otherwise
            - result: Dictionary containing Redis server status
    """
    try:
        result = subprocess.run(['systemctl', 'is-active', 'redis'], 
                              capture_output=True, 
                              text=True)
        status = result.stdout.strip()
        return True, {"status": status}
    except Exception as e:
        return False, f"Failed to check Redis status: {str(e)}"

# KAFKA
def get_kafka_topics_count(*args, **kwargs):
    """
    Gets the count of Kafka topics.
    
    Args:
        *args: Variable length argument list
        **kwargs: Arbitrary keyword arguments
    
    Returns:
        tuple: (success: bool, result: dict)
            - success: True if operation was successful, False otherwise
            - result: Dictionary containing topics_count
    """
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=config_dict['kafka_bootstrap_servers'])
        topics = admin_client.list_topics()
        admin_client.close()
        return True, {"topics_count": len(topics)}
    except Exception as e:
        return False, f"Failed to get topics count: {str(e)}"

def get_kafka_groups_count(*args, **kwargs):
    """
    Gets the count of Kafka consumer groups.
    
    Args:
        *args: Variable length argument list
        **kwargs: Arbitrary keyword arguments
    
    Returns:
        tuple: (success: bool, result: dict)
            - success: True if operation was successful, False otherwise
            - result: Dictionary containing groups_count
    """
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=config_dict['kafka_bootstrap_servers'])
        groups = admin_client.list_consumer_groups()
        admin_client.close()
        return True, {"groups_count": len(groups)}
    except Exception as e:
        return False, f"Failed to get groups count: {str(e)}"

def check_topics_groups_match(*args, **kwargs):
    """
    Checks if the number of Kafka topics matches the number of consumer groups.
    
    Args:
        *args: Variable length argument list
        **kwargs: Arbitrary keyword arguments
    
    Returns:
        tuple: (success: bool, result: dict)
            - success: True if operation was successful, False otherwise
            - result: Dictionary containing topics_count, groups_count, and match_status
    """
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=config_dict['kafka_bootstrap_servers'])
        topics = admin_client.list_topics()
        groups = admin_client.list_consumer_groups()
        admin_client.close()
        
        topics_count = len(topics)
        groups_count = len(groups)
        
        return True, {
            "topics_count": topics_count,
            "groups_count": groups_count,
            "match_status": topics_count == groups_count
        }
    except Exception as e:
        return False, f"Failed to check topics and groups match: {str(e)}"

def start_kafka(*args, **kwargs):
    """
    Starts the Kafka service using systemctl.
    
    Args:
        *args: Variable length argument list
        **kwargs: Arbitrary keyword arguments
    
    Returns:
        tuple: (success: bool, message: str)
            - success: True if Kafka started successfully, False otherwise
            - message: Status message describing the result
    """
    try:
        subprocess.run(['systemctl', 'start', 'kafka'], check=True)
        return True, "Kafka service started successfully"
    except subprocess.CalledProcessError as e:
        return False, f"Failed to start Kafka: {str(e)}"
    except Exception as e:
        return False, f"An unexpected error occurred while starting Kafka: {str(e)}"

def stop_kafka(*args, **kwargs):
    """
    Stops the Kafka service using systemctl.
    
    Args:
        *args: Variable length argument list
        **kwargs: Arbitrary keyword arguments
    
    Returns:
        tuple: (success: bool, message: str)
            - success: True if Kafka stopped successfully, False otherwise
            - message: Status message describing the result
    """
    try:
        pass
        # subprocess.run(['systemctl', 'stop', 'kafka'], check=True)
        return True, "Kafka service stopped successfully"
    except subprocess.CalledProcessError as e:
        return False, f"Failed to stop Kafka: {str(e)}"
    except Exception as e:
        return False, f"An unexpected error occurred while stopping Kafka: {str(e)}"

def check_kafka_status(*args, **kwargs):
    """
    Checks the status of the Kafka service using systemctl.
    
    Args:
        *args: Variable length argument list
        **kwargs: Arbitrary keyword arguments
    
    Returns:
        tuple: (success: bool, result: dict)
            - success: True if status check was successful, False otherwise
            - result: Dictionary containing status information
    """
    try:
        result = subprocess.run(['systemctl', 'is-active', 'kafka'], 
                              capture_output=True, 
                              text=True)
        status = result.stdout.strip()
        return True, {
            "status": status,
            "is_active": status == "active",
            "is_inactive": status == "inactive",
            "is_not_running": status == "failed"
        }
    except Exception as e:
        return False, f"Failed to check Kafka status: {str(e)}"

def delete_all_kafka_topics(*args, **kwargs):
    """
    Deletes all Kafka topics.
    
    Args:
        *args: Variable length argument list
        **kwargs: Arbitrary keyword arguments
    
    Returns:
        tuple: (success: bool, message: str)
            - success: True if topics were deleted successfully, False otherwise
            - message: Status message describing the result
    """
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=config_dict['kafka_bootstrap_servers'])
        topics = admin_client.list_topics()
        
        if not topics:
            admin_client.close()
            return True, "No topics found to delete"
            
        # Delete all topics
        admin_client.delete_topics(topics)
        
        admin_client.close()
        return True, f"Successfully deleted {len(topics)} topics"
    except Exception as e:
        return False, f"Failed to delete topics: {str(e)}"

def delete_specific_kafka_topic(topic_name, *args, **kwargs):
    """
    Deletes a specific Kafka topic.
    
    Args:
        topic_name (str): Name of the topic to delete
        *args: Variable length argument list
        **kwargs: Arbitrary keyword arguments
    
    Returns:
        tuple: (success: bool, message: str)
            - success: True if topic was deleted successfully, False otherwise
            - message: Status message describing the result
    """
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=config_dict['kafka_bootstrap_servers'])
        
        # Check if topic exists
        topics = admin_client.list_topics()
        if topic_name not in topics:
            admin_client.close()
            return False, f"Topic '{topic_name}' does not exist"
            
        # Delete the specific topic
        admin_client.delete_topics([topic_name])
        admin_client.close()
        return True, f"Successfully deleted topic '{topic_name}'"
    except Exception as e:
        return False, f"Failed to delete topic '{topic_name}': {str(e)}"

def run_create_topics(*args, **kwargs):
    """
    Runs the create_topics.py script to create Kafka topics.
    And then verifies from the log file that the topics were created successfully.
    verifies that the log file does not contain any errors. by doing a grep -i "err"

    Returns:
        tuple: (success: bool, message: str)
    """
    try:
        command = ['python3', 'create_topics.py', PANELS_FILE_PATH, str(NUM_PARTITIONS)]
        
        with open(TOPIC_CREATION_LOG, 'w') as log_file:
            result = subprocess.run(
                command,
                stdout=log_file,
                stderr=subprocess.PIPE,
                text=True
            )
        
        if result.returncode != 0:
            return False, f"Failed to create topics: {result.stderr}"
            
        # Check log file for error messages
        with open(TOPIC_CREATION_LOG, 'r') as f:
            if 'err' in f.read().lower():
                return False, "Errors found in the log file"
        
        return True, "Topics created successfully"
    except Exception as e:
        print(f"Error running create_topics.py: {str(e)}")
        return False, f"Error running create_topics.py: {str(e)}"

def run_validate_topics(*args, **kwargs):
    """
    Runs the validate_topics.py script to validate Kafka topics.
    And then verifies from the log file that the topics were created successfully.
    verifies that the log file does not contain any errors. by doing a grep -i "err"
    
    Returns:
        tuple: (success: bool, message: str)
    """
    try:
        command = ['python3', 'validate_topics.py', PANELS_FILE_PATH, str(NUM_PARTITIONS)]
        
        with open(TOPIC_VALIDATION_LOG, 'w') as log_file:
            result = subprocess.run(
                command,
                stdout=log_file,
                stderr=subprocess.PIPE,
                text=True
            )
        
        if result.returncode != 0:
            return False, f"Failed to validate topics: {result.stderr}"
            
        # Check log file for error messages
        with open(TOPIC_VALIDATION_LOG, 'r') as f:
            if 'err' in f.read().lower():
                return False, "Errors found in the log file"
        
        return True, "Topics validated successfully"
    except Exception as e:
        return False, f"Error running validate_topics.py: {str(e)}"

def clear_kafka_directories(*args, **kwargs):
    """
    Clears Kafka and Zookeeper data directories by:
    1. Stopping Kafka and Zookeeper services
    2. Removing all files from data directories
    3. Starting Kafka and Zookeeper services
    
    Args:
        *args: Variable length argument list
        **kwargs: Arbitrary keyword arguments
    
    Returns:
        tuple: (success: bool, message: str)
            - success: True if operation was successful, False otherwise
            - message: Status message describing the result
    """
    try:
        # Stop services
        kafka_stop_success, kafka_stop_msg = stop_kafka()
        zookeeper_stop_success, zookeeper_stop_msg = stop_zookeeper()
        print(f"Kafka stop success: {kafka_stop_success}, Kafka stop msg: {kafka_stop_msg}")
        
        if not kafka_stop_success or not zookeeper_stop_success:
            return False, f"Failed to stop services: Kafka - {kafka_stop_msg}, Zookeeper - {zookeeper_stop_msg}"
        
        # Clear directories
        kafka_logs_dir = '/data/kafka-logs'
        zookeeper_dir = '/data/zookeeper'
        
        # Remove all files including hidden ones using os.system
        # Ignore the "refusing to remove . or .." messages as they are not errors
        kafka_clear_cmd = f"sudo rm -rf {kafka_logs_dir}/* {kafka_logs_dir}/.* 2>/dev/null"
        zookeeper_clear_cmd = f"sudo rm -rf {zookeeper_dir}/* {zookeeper_dir}/.* 2>/dev/null"
        
        # kafka_clear_status = os.system(kafka_clear_cmd)
        # zookeeper_clear_status = os.system(zookeeper_clear_cmd)
        
        # Check if directories are empty (excluding . and ..)
        kafka_empty = len(os.listdir(kafka_logs_dir)) <= 2  # . and .. are always present
        zookeeper_empty = len(os.listdir(zookeeper_dir)) <= 2
        
        if not kafka_empty or not zookeeper_empty:
            return False, "Failed to clear Kafka or Zookeeper directories"
        
        # Start services
        kafka_start_success, kafka_start_msg = start_kafka()
        zookeeper_start_success, zookeeper_start_msg = start_zookeeper()
        
        if not kafka_start_success or not zookeeper_start_success:
            return False, f"Failed to start services: Kafka - {kafka_start_msg}, Zookeeper - {zookeeper_start_msg}"

        return True, "Successfully cleared Kafka and Zookeeper directories and restarted services"
        
    except Exception as e:
        return False, f"An unexpected error occurred: {str(e)}"

def start_zookeeper(*args, **kwargs):
    """
    Starts the Zookeeper service using systemctl.
    
    Args:
        *args: Variable length argument list
        **kwargs: Arbitrary keyword arguments
    
    Returns:
        tuple: (success: bool, message: str)
            - success: True if Zookeeper started successfully, False otherwise
            - message: Status message describing the result
    """
    try:
        subprocess.run(['systemctl', 'start', 'zookeeper'], check=True)
        return True, "Zookeeper service started successfully"
    except subprocess.CalledProcessError as e:
        return False, f"Failed to start Zookeeper: {str(e)}"
    except Exception as e:
        return False, f"An unexpected error occurred while starting Zookeeper: {str(e)}"

def stop_zookeeper(*args, **kwargs):
    """
    Stops the Zookeeper service using systemctl.
    
    Args:
        *args: Variable length argument list
        **kwargs: Arbitrary keyword arguments
    
    Returns:
        tuple: (success: bool, message: str)
            - success: True if Zookeeper stopped successfully, False otherwise
            - message: Status message describing the result
    """
    try:
        pass
        # subprocess.run(['systemctl', 'stop', 'zookeeper'], check=True)
        return True, "Zookeeper service stopped successfully"
    except subprocess.CalledProcessError as e:
        return False, f"Failed to stop Zookeeper: {str(e)}"
    except Exception as e:
        return False, f"An unexpected error occurred while stopping Zookeeper: {str(e)}"

# LLM 
def identify_panels(text: str, *args, **kwargs) -> list[str]:
    """
    Identifies and returns panels from the given text.
    Panels can be comma-separated or line-separated.
    Uses the provided LLM to extract and refine the panel list.

    Args:
        text: The input text containing potential panel information.
        llm: An initialized Langchain ChatGoogleGenerativeAI model.

    Returns:
        A list of identified panel names.
    """
    try:
        prompt = f"""You are an expert in identifying distinct panels from text.
        Given the following text, identify all the individual panel names.
        Panels can be separated by commas or appear on separate lines.

        Text:
        \"{text}\"

        Return the identified panels as a comma-separated list."""

        response = llm.invoke(prompt)
        extracted_panels_str = response.content

        # Split the extracted string by comma and then strip whitespace
        panels = [panel.strip() for panel in extracted_panels_str.split(',')]

        # Further refine by splitting by newline in case the LLM included them
        refined_panels = []
        for panel in panels:
            refined_panels.extend([p.strip() for p in panel.split('\n') if p.strip()])

        # Remove any empty strings that might have resulted from splitting
        return [panel for panel in refined_panels if panel]

    except Exception as e:
        print(f"An error occurred during panel identification: {e}")
        return []


def identify_panels_and_cids(text: str, *args, **kwargs) -> list[str]:
    """
    Identifies and returns panels and cids from the given text.
    Panels and cids can be given in a csv format or python list of tuples format.
    Uses the provided LLM to extract and refine the panel list.

    Args:
        text: The input text containing potential panel information.
        llm: An initialized Langchain ChatGoogleGenerativeAI model.

    Returns:
        A list of identified panel names and cids.
    """
    try:
        prompt = f"""You are an expert in identifying distinct panels and cids from text.
        Given the following text, identify all the individual panel names and cids.
        Panels and cids can be separated by commas or appear on separate lines.

        Text:
        \"{text}\"

        Return the identified panels and cids as a csv format."""

        response = llm.invoke(prompt)
        extracted_panels_str = response.content

        # extract the panels and cids from the response
        panels_cids = []
        for line in extracted_panels_str.split('\n'):
            if line.strip():
                parts = line.strip().split(',')
                if len(parts) == 2:
                    panels_cids.append((parts[0].strip(), parts[1].strip()))

        return panels_cids

    except Exception as e:
        print(f"An error occurred during panel identification: {e}")
        return []


def identify_methods(text: str) -> list[str]:
    """
    Identifies and returns methods from the given text.
    Methods can be comma-separated or line-separated.
    Uses the provided LLM to extract and refine the method list.

    Args:
        text: The input text containing potential panel information.
        llm: An initialized Langchain ChatGoogleGenerativeAI model.

    Returns:
        A list of identified method names.
    """
    try:
        prompt = f"""You are an expert in identifying distinct methods from text.
        Given the following text, identify all the individual method names.
        Methods can be separated by commas or appear on separate lines.

        Text:
        \"{text}\"

        Return the identified methods as a comma-separated list."""

        response = llm.invoke(prompt)
        extracted_methods_str = response.content

        # Split the extracted string by comma and then strip whitespace
        methods = [method.strip() for method in extracted_methods_str.split(',')]

        # Further refine by splitting by newline in case the LLM included them
        refined_methods = []
        for method in methods:
            refined_methods.extend([m.strip() for m in method.split('\n') if m.strip()])

        # Remove any empty strings that might have resulted from splitting
        return [method for method in refined_methods if method]

    except Exception as e:
        print(f"An error occurred during method identification: {e}")
        return []

# OTHER HELPER FUNCTIONS
def create_panels_file(text: list, *args, **kwargs) -> tuple[bool, str]:
    """
    Extracts panels from the given text and creates a panels.txt file with one panel per line.
    
    Args:
        text (str): Text containing panel information
        *args: Variable length argument list
        **kwargs: Arbitrary keyword arguments
    
    Returns:
        tuple: (success: bool, message: str)
            - success: True if file was created successfully, False otherwise
            - message: Status message describing the result
    """
    try:
        if not isinstance(text, str):
            return False, "Input must be a string"

        panels = identify_panels(text, llm)

        panels_file_path = os.path.join(BASE_DIR, 'panels.txt')
        
        with open(panels_file_path, 'w') as f:
            for panel in panels:
                if not isinstance(panel, str):
                    return False, f"Invalid panel format: {panel}"
                f.write(f"{panel}\n")
                
        return True, f"Successfully created panels file at {panels_file_path}"
    except Exception as e:
        return False, f"Failed to create panels file: {str(e)}"


def create_panels_cid_csv_file(text: list, *args, **kwargs) -> tuple[bool, str]:
    """
    Extracts panels and cids from the given text and creates a panels_cid.csv file with one panel,cid per line.
    
    Args:
        text (str): Text containing panel information
        *args: Variable length argument list
        **kwargs: Arbitrary keyword arguments
    
    Returns:
        tuple: (success: bool, message: str)
            - success: True if file was created successfully, False otherwise
            - message: Status message describing the result
    """
    try:
        if not isinstance(text, str):
            return False, "Input must be a string"

        panels_cids = identify_panels_and_cids(text, llm)

        panels_file_path = PANELS_CID_CSV_FILE_PATH
        
        with open(panels_file_path, 'w') as f:
            for panel_cid in panels_cids:
                if not isinstance(panel_cid, tuple):
                    return False, f"Invalid panel format: {panel_cid}"
                f.write(f"{panel_cid[0]},{panel_cid[1]}\n")
                
        return True, f"Successfully created panels_cids file at {panels_file_path}"
    except Exception as e:
        return False, f"Failed to create panels_cids file: {str(e)}"

def get_panels_file_length(*args, **kwargs) -> tuple[bool, dict]:
    """
    Gets the number of panels in the panels.txt file.
    
    Args:
        *args: Variable length argument list
        **kwargs: Arbitrary keyword arguments
    
    Returns:
        tuple: (success: bool, result: dict)
            - success: True if operation was successful, False otherwise
            - result: Dictionary containing panels_count and file_path
    """
    try:
        panels_file_path = os.path.join(BASE_DIR, 'panels.txt')
        
        if not os.path.exists(panels_file_path):
            return False, "Panels file does not exist"
            
        with open(panels_file_path, 'r') as f:
            panels = [line.strip() for line in f if line.strip()]
            
        return True, {
            "panels_count": len(panels),
            "file_path": panels_file_path
        }
    except Exception as e:
        return False, f"Failed to get panels count: {str(e)}"

def delete_panels_file(*args, **kwargs) -> tuple[bool, str]:
    """
    Deletes the panels.txt file from the BASE_DIR.
    
    Args:
        *args: Variable length argument list
        **kwargs: Arbitrary keyword arguments
    
    Returns:
        tuple: (success: bool, message: str)
            - success: True if file was deleted successfully, False otherwise
            - message: Status message describing the result
    """
    try:
        panels_file_path = os.path.join(BASE_DIR, 'panels.txt')
        
        if not os.path.exists(panels_file_path):
            return False, "Panels file does not exist"
            
        os.remove(panels_file_path)
        return True, "Successfully deleted panels file"
    except Exception as e:
        return False, f"Failed to delete panels file: {str(e)}"

def clean_migration_logs(*args, **kwargs) -> tuple[bool, str]:
    """
    Cleans the migration logs directory by:
    1. Creating a backup folder with current timestamp
    2. Moving all files (not folders) to the backup folder
    
    Returns:
        tuple: (success: bool, message: str)
            - success: True if operation was successful, False otherwise
            - message: Status message describing the result
    """
    try:
        # Create backup directory with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_dir = os.path.join(BASE_MIGRATION_LOG_DIR, f"migration_logs_bkp_{timestamp}")
        
        # Create backup directory if it doesn't exist
        os.makedirs(backup_dir, exist_ok=True)
        
        # Move all files (not directories) to backup directory
        for item in os.listdir(BASE_MIGRATION_LOG_DIR):
            item_path = os.path.join(BASE_MIGRATION_LOG_DIR, item)
            if os.path.isfile(item_path):
                shutil.move(item_path, os.path.join(backup_dir, item))
                
        return True, f"Successfully backed up logs to {backup_dir}"
    except Exception as e:
        return False, f"Failed to clean migration logs: {str(e)}"


def get_migrating_panels(*args, **kwargs) -> tuple[bool, list]:
    """
    Gets the panels that are currently being migrated.
    """
    try:
        # Get all files in the migration logs directory
        migration_logs_dir = os.path.join(BASE_DIR, 'panels.txt')
        
        # read the panels.txt file
        with open(migration_logs_dir, 'r') as f:
            panels = f.readlines()
        
        # Filter out non-files and directories
        return True, [panel.strip() for panel in panels if panel.strip()]

    except Exception as e:
        return False, f"Failed to get migrating panels: {str(e)}"

def check_migration_processes(*args, **kwargs) -> tuple[bool, dict]:
    """
    Checks if any migration processes are running by checking for:
    1. run_producer
    2. run_consumer
    3. kill_consumer
    4. java write process
    5. java read process
    
    Returns:
        tuple: (success: bool, result: dict)
            - success: True if check was successful, False otherwise
            - result: Dictionary containing process status or error message
    """
    try:
        processes = {
            'run_producer': False,
            'run_consumer': False,
            'kill_consumer': False,
            'java_write': False,
            'java_read': False
        }
        
        # Check for each process
        for process in ['run_producer', 'run_consumer', 'kill_consumer']:
            pass
            # result = subprocess.run(
            #     ['ps', '-eaf', '|', 'grep', process],
            #     capture_output=True,
            #     text=True
            # )
            # result = os.system(f"ps -eaf | grep {process}")
            # print('result.stdout', result)
            # # If grep finds itself and the process, count > 1
            # processes[process] = len(result.stdout.splitlines()) >= 1
            
        # Check for java write process
        # result = subprocess.run(
        #     ['ps', '-eaf', '|', 'grep', 'java', '|', 'grep', 'write'],
        #     capture_output=True,
        #     text=True
        # )
        # result = os.system(f"ps -eaf | grep java | grep write")
        # print('result.stdout', result)
        # processes['java_write'] = len(result.stdout.splitlines()) >= 1
        
        # Check for java read process
        # result = subprocess.run(
        #     ['ps', '-eaf', '|', 'grep', 'java', '|', 'grep', 'read'],
        #     capture_output=True,
        #     text=True
        # )
        # result = os.system(f"ps -eaf | grep java | grep read")
        # print('result.stdout', result)
        # processes['java_read'] = len(result.stdout.splitlines()) >= 1
            
        return True, processes
    except Exception as e:
        return False, f"Failed to check migration processes: {str(e)}"

def kill_migration_processes(*args, **kwargs) -> tuple[bool, str]:
    """
    Kills any running migration processes:
    1. run_producer
    2. run_consumer
    3. kill_consumer
    
    Returns:
        tuple: (success: bool, message: str)
    """
    try:
        processes = ['run_producer', 'run_consumer', 'kill_consumer']
        killed = []
        
        for process in processes:
            # First command: ps -eaf
            ps_process = subprocess.Popen(['ps', '-eaf'], stdout=subprocess.PIPE)
            
            # Second command: grep the process (reading from the output of ps)
            grep_process = subprocess.Popen(
                ['grep', process],
                stdin=ps_process.stdout,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            # Close the stdout of the first process
            ps_process.stdout.close()
            
            # Get the output and error from grep
            stdout, stderr = grep_process.communicate()
            
            # Skip the grep process itself
            lines = stdout.decode().splitlines()
            for line in lines:
                if process in line and 'grep' not in line:
                    pid = line.split()[1]  # Get the PID
                    subprocess.run(['kill', '-15', pid])
                    killed.append(f"{process} (PID: {pid})")
        
        if killed:
            killed_str = "\n".join(killed)
            return True, f"Successfully killed processes:\n{killed_str}"
        return True, "No migration processes were running"
    except Exception as e:
        return False, f"Failed to kill migration processes: {str(e)}"

def push_panels_info_to_redis(*args, **kwargs) -> tuple[bool, str]:
    """
    Pushes panels to Redis using push_panels_to_redis.py script
    and verifies the output log for errors.
    
    Returns:
        tuple: (success: bool, message: str)
            - success: True if panels were pushed successfully, False otherwise
            - message: Status message describing the result
    """
    try:
        result = subprocess.run(
            ['python3', os.path.join(BASE_DIR, 'push_panels_to_redis.py'),
             os.path.join(BASE_DIR, 'panels.txt'),
             os.path.join(BASE_DIR, 'logs/push.log'), '1'],
            capture_output=True,
            text=True
        )
        
        # Check for errors in the output
        if result.returncode != 0:
            return False, f"Failed to push panels to Redis: {result.stderr}"
            
        # Check log file for error messages and get the latest info line
        latest_info_line = None
        with open(os.path.join(BASE_DIR, 'logs/push.log'), 'r') as f:
            for line in f.readlines():
                if 'panels to redis' in line.lower() and 'info' in line.lower():
                    latest_info_line = line.strip()
        
        if latest_info_line:
            pushed = latest_info_line.split('pushed')[1].strip()
            pushed_cnt = int(pushed.split(' ')[0])
            success, total_result = get_panels_file_length()
            if not success:
                return False, f"Failed to get total panels count: {total_result}"
            total_cnt = total_result['panels_count']
            
            if pushed_cnt == total_cnt:
                return True, f"Successfully pushed {pushed_cnt} panels to Redis"
            else:
                return False, f"Failed to push {total_cnt-pushed_cnt} panels to Redis and successfully pushed {pushed_cnt} panels"
        else:
            return False, "No panel push information found in log file"
    except Exception as e:
        return False, f"Failed to push panels to Redis: {str(e)}"

def pre_migration_check(*args, **kwargs) -> tuple[bool, str]:
    """
    Performs pre-migration checks and preparation for migration:
    1. Health check
    2. Redis cleanup and verification
    3. Kafka cleanup, topic creation and validation
    4. Log folder cleanup
    5. Time series collections validation
    6. Push panels to Redis
    7. Check for running migration processes
    8. Final health check
    
    Returns:
        tuple: (success: bool, message: str)
    """
    try:
        # 1. Initial health check
        if not health_check(PROPERTY_FILE, HEALTH_CHECK_LOG):
            return False, "Health check failed, please check"
            
        # 2. Redis cleanup and verification
        success, message = delete_all_keys()
        if not success:
            return False, f"Failed to clear Redis: {message}"
            
        success, result = get_total_keys()
        if not success:
            return False, f"Failed to verify Redis keys: {result}"
        if result['total_keys'] != 0:
            return False, "Redis keys not cleared properly"
            
        # 3. Kafka cleanup and topic setup
        success, message = delete_all_kafka_topics()
        if not success:
            return False, f"Failed to delete Kafka topics: {message}"
            
        # Create topics
        success, message = run_create_topics()
        if not success:
            return False, f"Failed to create Kafka topics: {message}"
            
        # Validate topics
        success, message = run_validate_topics()
        if not success:
            return False, f"Failed to validate Kafka topics: {message}"
            
        # 4. Clean log folder
        success, message = clean_migration_logs()
        if not success:
            return False, f"Failed to clean log folder: {message}"
            
        # 5. Verify time series collections
        success, message = validate_time_series_collections()
        if not success:
            return False, f"Failed to validate time series collections: {message}"
            
        # 6. Push panels to Redis
        success, message = push_panels_info_to_redis()
        if not success:
            return False, f"Failed to push panels to Redis: {message}"
            
        # 7. Check for running migration processes
        success, result = check_migration_processes()
        if not success:
            return False, f"Failed to check migration processes: {result}"
            
        for process, running in result.items():
            if running:
                return False, f"Migration process {process} is still running"
                
        # 8. Final health check
        if not health_check(PROPERTY_FILE, HEALTH_CHECK_LOG):
            return False, "Final health check failed, please check"
            
        return True, "All pre-migration checks passed successfully"
    except Exception as e:
        return False, f"Pre-migration check failed: {str(e)}"

def start_producer_processes(*args, **kwargs) -> tuple[bool, str]:
    """
    Starts producer process and verifies its status.
    
    Args:
        log_file (str): Path to the log file
    
    Returns:
        tuple: (success: bool, message: str)
    """
    try:
        script = 'run_producer.py'
        cmd = [
            'python3',
            os.path.join(BASE_DIR, script),
            os.path.join(BASE_DIR, 'logs', RUN_PRODUCER_LOG)
        ]
        subprocess.Popen(cmd)
        
        time.sleep(2)  # Give process time to start
        
        # Check process status
        result = subprocess.run(
            f'ps -eaf | grep {script}',
            shell=True,
            capture_output=True,
            text=True
        )
        
        # Count actual processes (excluding grep itself)
        process_lines = [line for line in result.stdout.splitlines() if script in line and 'grep' not in line]
        
        if len(process_lines) == 1:
            return True, "Successfully started producer process\n" + result.stdout
        else:
            return False, f"Expected 1 producer process but found {len(process_lines)}"
            
    except Exception as e:
        return False, f"Failed to start producer process: {str(e)}"


def start_producer_processes_for_specific_methods(text: str, *args, **kwargs) -> tuple[bool, str]:
    """
    Starts producer process and verifies its status for specific methods.
    
    Args:
        text (str): Text to start producer for
    Returns:
        tuple: (success: bool, message: str)
    """
    try:
        methods = identify_methods(text)
        script = 'run_producer.py'
        cmd = [
            'python3',
            os.path.join(BASE_DIR, script),
            os.path.join(BASE_DIR, 'logs', RUN_PRODUCER_LOG),
            '--methods',
            ','.join(methods)
        ]
        subprocess.Popen(cmd)
        
        time.sleep(2)  # Give process time to start
        
        # Check process status
        result = subprocess.run(
            f'ps -eaf | grep {script}',
            shell=True,
            capture_output=True,
            text=True
        )
        
        # Count actual processes (excluding grep itself)
        process_lines = [line for line in result.stdout.splitlines() if script in line and 'grep' not in line]
        
        if len(process_lines) == 1:
            return True, "Successfully started producer process\n" + result.stdout
        else:
            return False, f"Expected 1 producer process but found {len(process_lines)}"
            
    except Exception as e:
        return False, f"Failed to start producer process: {str(e)}"

def start_consumer_processes(*args, **kwargs) -> tuple[bool, str]:
    """
    Starts consumer process and verifies its status.
    
    Args:
        log_file (str): Path to the log file
    
    Returns:
        tuple: (success: bool, message: str)
    """
    try:
        script = 'run_consumer.py'
        cmd = [
            'python3',
            os.path.join(BASE_DIR, script),
            os.path.join(BASE_DIR, 'logs', RUN_CONSUMER_LOG)
        ]
        subprocess.Popen(cmd)
        
        time.sleep(2)  # Give process time to start
        
        # Check process status
        result = subprocess.run(
            f'ps -eaf | grep {script}',
            shell=True,
            capture_output=True,
            text=True
        )
        
        # Count actual processes (excluding grep itself)
        process_lines = [line for line in result.stdout.splitlines() if script in line and 'grep' not in line]
        
        if len(process_lines) == 1:
            return True, "Successfully started consumer process\n" + result.stdout
        else:
            return False, f"Expected 1 consumer process but found {len(process_lines)}"
            
    except Exception as e:
        return False, f"Failed to start consumer process: {str(e)}"


def start_consumer_processes_for_specific_methods(text: str, *args, **kwargs) -> tuple[bool, str]:
    """
    Starts consumer process and verifies its status for specific methods.
    
    Args:
        text (str): Text to start consumer for
    
    Returns:
        tuple: (success: bool, message: str)
    """
    try:
        methods = identify_methods(text)
        script = 'run_consumer.py'
        cmd = [
            'python3',
            os.path.join(BASE_DIR, script),
            os.path.join(BASE_DIR, 'logs', RUN_CONSUMER_LOG),
            '--methods',
            ','.join(methods)
        ]
        subprocess.Popen(cmd)
        
        time.sleep(2)  # Give process time to start
        
        # Check process status
        result = subprocess.run(
            f'ps -eaf | grep {script}',
            shell=True,
            capture_output=True,
            text=True
        )
        
        # Count actual processes (excluding grep itself)
        process_lines = [line for line in result.stdout.splitlines() if script in line and 'grep' not in line]
        
        if len(process_lines) == 1:
            return True, "Successfully started consumer process\n" + result.stdout
        else:
            return False, f"Expected 1 consumer process but found {len(process_lines)}"
            
    except Exception as e:
        return False, f"Failed to start consumer process: {str(e)}"


def start_kill_consumer_processes(*args, **kwargs) -> tuple[bool, str]:
    """
    Starts kill consumer process and verifies its status.
    
    Args:
        log_file (str): Path to the log file
    
    Returns:
        tuple: (success: bool, message: str)
    """
    try:
        script = 'kill_consumer.py'
        cmd = [
            'python3',
            os.path.join(BASE_DIR, script),
            os.path.join(BASE_DIR, 'panels.txt'),
            os.path.join(BASE_DIR, 'logs', KILL_CONSUMER_LOG),
            config_dict['env']
        ]
        subprocess.Popen(cmd)
        
        time.sleep(2)  # Give process time to start
        
        # Check process status
        result = subprocess.run(
            f'ps -eaf | grep {script}',
            shell=True,
            capture_output=True,
            text=True
        )
        
        # Count actual processes (excluding grep itself)
        process_lines = [line for line in result.stdout.splitlines() if script in line and 'grep' not in line]
        
        if len(process_lines) == 1:
            return True, "Successfully started kill consumer process\n" + result.stdout
        else:
            return False, f"Expected 1 kill consumer process but found {len(process_lines)}"
            
    except Exception as e:
        return False, f"Failed to start kill consumer process: {str(e)}"

def start_migration_processes(*args, **kwargs) -> tuple[bool, str]:
    """
    Starts migration processes and verifies their status:
    Also can be used to add more processes or clients to the migration
    1. run_producer.py
    2. run_consumer.py
    3. kill_consumer.py
    
    Returns:
        tuple: (success: bool, message: str)
            - success: True if all processes started successfully, False otherwise
            - message: Status message describing the result
    """
    try:
        # Start producer process
        success, message = start_producer_processes()
        if not success:
            return False, message
            
        # Start consumer process
        success, message = start_consumer_processes()
        if not success:
            return False, message
            
        # Start kill consumer process
        success, message = start_kill_consumer_processes()
        if not success:
            return False, message
            
        # Get final process status by checking each process separately
        processes = {
            'run_producer.py': False,
            'run_consumer.py': False,
            'kill_consumer.py': False
        }
        
        return_result = ""
        for script in processes.keys():
            result = subprocess.run(
                f'ps -eaf | grep {script}',
                shell=True,
                capture_output=True,
                text=True
            )
            process_lines = [line for line in result.stdout.splitlines() if script in line and 'grep' not in line]
            processes[script] = len(process_lines) == 1
            return_result += result.stdout
        all_running = all(processes.values())
        if all_running:
            return True, "Successfully started all migration processes\n" + return_result
        else:
            not_running = [script for script, running in processes.items() if not running]
            return False, f"Failed to start processes: {', '.join(not_running)}"
            
    except Exception as e:
        return False, f"Failed to start migration processes: {str(e)}"

def check_migration_concurrency(*args, **kwargs) -> tuple[bool, str]:
    """
    Checks the concurrency of the migration by counting running processes:
    1. run_producer.py
    2. run_consumer.py
    
    Returns:
        tuple: (success: bool, result: str)
            - success: True if check was successful, False otherwise
            - result: Message describing the concurrency status or error message
    """
    try:
        # Check producer processes
        ps_process = subprocess.Popen(['ps', '-eaf'], stdout=subprocess.PIPE)
        grep_process = subprocess.Popen(
            ['grep', 'run_producer.py'],
            stdin=ps_process.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        ps_process.stdout.close()
        stdout, stderr = grep_process.communicate()
        producer_count = len([line for line in stdout.decode().splitlines() if 'grep' not in line])
        
        # Check consumer processes
        ps_process = subprocess.Popen(['ps', '-eaf'], stdout=subprocess.PIPE)
        grep_process = subprocess.Popen(
            ['grep', 'run_consumer.py'],
            stdin=ps_process.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        ps_process.stdout.close()
        stdout, stderr = grep_process.communicate()
        consumer_count = len([line for line in stdout.decode().splitlines() if 'grep' not in line])
        
        if producer_count == consumer_count:
            return True, f"Currently migration concurrency is {producer_count}"
        else:
            return False, f"Concurrency mismatch: {producer_count} producers and {consumer_count} consumers running"
    except Exception as e:
        return False, f"Failed to check migration concurrency: {str(e)}"

def validate_time_series_collections(*args, **kwargs) -> tuple[bool, str]:
    """
    Validates time series indexes by running ts_mongo_ind_index_validation.py
    and checks the output log for errors.
    
    Returns:
        tuple: (success: bool, message: str)
            - success: True if validation passed without errors, False otherwise
            - message: Status message describing the result
    """
    try:
        # Run the validation script and redirect output to log file
        with open(TS_COLLECTION_VALIDATION_LOG, 'w') as f:
            result = subprocess.run(
                ['python3', os.path.join(BASE_DIR, 'ts_mongo_ind_index_validation.py'), PANELS_FILE_PATH],
                text=True,
                stdout=f,
                stderr=f
            )
        
        # Check for errors in the output
        if result.returncode != 0:
            return False, f"Validation script failed with return code {result.returncode}"
            
        # Check for errors in the output
        if result.returncode != 0:
            return False, f"Validation script failed with return code {result.returncode}"
            
        # Check log file for error messages
        with open(TS_COLLECTION_VALIDATION_LOG, 'r') as f:
            log_content = f.read().lower()
            if 'error' in log_content or 'err' in log_content:
                return False, "Errors found in time series index validation log"
                
        return True, "Time series indexes validated successfully"
    except Exception as e:
        return False, f"Failed to validate time series indexes: {str(e)}"


def run_health_check(*args, **kwargs) -> tuple[bool, str]:
    """
    Runs the health_check.py script and returns the result.
    """
    return health_check(PROPERTY_FILE, HEALTH_CHECK_LOG)

def get_kafka_status(*args, **kwargs) -> tuple[bool, str]:
    """
    Runs the Kafka status script and returns the output.
    """
    KAFKA_SCRIPT_PATH = "/home/mongodb/smart_migration/tabulate_data.py"
    try:
        # Ensure the script has execute permissions (important for security)
        subprocess.run(["chmod", "+x", KAFKA_SCRIPT_PATH], check=True)
        result = subprocess.run(["python3", KAFKA_SCRIPT_PATH, "/home/mongodb/smart_migration/" + 'panels.txt'],  # Pass the argument
                                 capture_output=True, text=True, check=True)
        return True, result.stdout
    except subprocess.CalledProcessError as e:
        return False, f"Error running Kafka status script: {e.stderr}"
    except FileNotFoundError:
        return False, f"Kafka status script not found at {KAFKA_SCRIPT_PATH}"
    except Exception as e:
        return False, f"An unexpected error occurred: {e}"

def format(kafka_status_output):
    """
    Formats the raw Kafka status output into a more readable message.

    Args:
        kafka_status_output (str): The raw string output from the Kafka status check.

    Returns:
        str or None: The formatted message string if data is found, otherwise None.
    """
    if "No data found" in kafka_status_output:
        return None
    else:
        # Remove the initial "(True, " and the trailing ")"
        cleaned_output = kafka_status_output.lstrip("(True, ").rstrip(")")
        message = "\nKafka Status:\n"
        message += f"```\n{cleaned_output}\n```"
        return message

def get_migration_status(*args, **kwargs) -> tuple[bool, str]:
    """
    Get the status of the migration in a table format.
    """
    try:
        total_pending_count = 0
        ENV = config_dict.get('env', 'dev')

        
        kafka_status_output = get_kafka_status()
        message = ""

        if "No data found" not in kafka_status_output:
            message += "\nKafka Status:\n"
            message += f"```{kafka_status_output}```" 

        return True, format(kafka_status_output)

    except Exception as e:
        return False, f"Failed to get migration status: {str(e)}"

def backup_redis_data(*args, **kwargs) -> tuple[bool, str]:
    """
    Takes backup of Redis data in human-readable format for GET, HGETALL, and LRANGE operations.
    
    Returns:
        tuple: (success: bool, message: str)
    """
    try:
        # Create backup directory if it doesn't exist
        os.makedirs(REDIS_BACKUP_DIR, exist_ok=True)
        
        # Generate timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create backup files for all operations
        get_filename = f"redis_backup_get_{timestamp}.txt"
        hgetall_filename = f"redis_backup_hgetall_{timestamp}.txt"
        queue_filename = f"redis_backup_queues_{timestamp}.txt"
        
        get_filepath = os.path.join(REDIS_BACKUP_DIR, get_filename)
        hgetall_filepath = os.path.join(REDIS_BACKUP_DIR, hgetall_filename)
        queue_filepath = os.path.join(REDIS_BACKUP_DIR, queue_filename)
        
        # Get Redis connection details from config
        redis_host = config_dict.get('redis_uri', 'localhost')
        redis_port = config_dict.get('redis_port', '6379')
        
        # Backup GET operations using redis-cli
        get_cmd = f"redis-cli -h {redis_host} -p {redis_port} --scan | while read key; do result=$(redis-cli -h {redis_host} -p {redis_port} GET \"$key\" 2>/dev/null); [ -n \"$result\" ] && echo \"$key: $result\"; done > {get_filepath}"
        subprocess.run(get_cmd, shell=True, check=True)
        
        # Backup HGETALL operations using redis-cli
        hgetall_cmd = f"redis-cli -h {redis_host} -p {redis_port} --scan | while read key; do result=$(redis-cli -h {redis_host} -p {redis_port} HGETALL \"$key\" 2>/dev/null); [ -n \"$result\" ] && echo \"$key:\" && echo \"$result\" && echo \"\"; done > {hgetall_filepath}"
        subprocess.run(hgetall_cmd, shell=True, check=True)
        
        # Backup Queue operations using redis-cli
        queue_cmd = f"redis-cli -h {redis_host} -p {redis_port} --scan | while read key; do result=$(redis-cli -h {redis_host} -p {redis_port} LRANGE \"$key\" 0 -1 2>/dev/null); [ -n \"$result\" ] && echo \"Queue: $key: $result\"; done > {queue_filepath}"
        subprocess.run(queue_cmd, shell=True, check=True)
        
        # Add headers to the files
        with open(get_filepath, 'r+') as f:
            content = f.read()
            f.seek(0, 0)
            f.write(f"Redis Backup - GET Operations\nTimestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n{'='*80}\n\n{content}")
        
        with open(hgetall_filepath, 'r+') as f:
            content = f.read()
            f.seek(0, 0)
            f.write(f"Redis Backup - HGETALL Operations\nTimestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n{'='*80}\n\n{content}")
        
        with open(queue_filepath, 'r+') as f:
            content = f.read()
            f.seek(0, 0)
            f.write(f"Redis Backup - Queue Operations\nTimestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n{'='*80}\n\n{content}")
        
        return True, f"Successfully created Redis backups:\nGET: {get_filepath}\nHGETALL: {hgetall_filepath}\nQueues: {queue_filepath}"
    except subprocess.CalledProcessError as e:
        return False, f"Failed to execute Redis CLI command: {str(e)}"
    except Exception as e:
        return False, f"Failed to create Redis backups: {str(e)}"

tools = [
    Tool.from_function(func=start_redis, name="start_redis", description="Starts the Redis server."),
    Tool.from_function(func=stop_redis, name="stop_redis", description="Stops the Redis server."),
    Tool.from_function(func=delete_keys_by_pattern, name="delete_keys_by_pattern", description="Deletes Redis keys matching a given pattern."),
    Tool.from_function(func=delete_producer_keys, name="delete_producer_keys", description="Deletes all Redis producer keys."),
    Tool.from_function(func=delete_consumer_keys, name="delete_consumer_keys", description="Deletes all Redis consumer keys."),
    Tool.from_function(func=delete_all_keys, name="delete_all_keys", description="Deletes all keys in Redis."),
    Tool.from_function(func=delete_specific_key, name="delete_specific_key", description="Deletes a specific Redis key."),
    Tool.from_function(func=get_methods_count_form_redis, name="get_methods_count_form_redis", description="Gets the count of redis keys referred as methods, starting with 'read' and 'write'"),
    Tool.from_function(func=get_total_keys, name="get_total_keys", description="Gets the total number of keys in Redis."),
    Tool.from_function(func=get_panels_length, name="get_panels_length", description="Get the length of the panels list in Redis"),
    Tool.from_function(func=check_redis_status, name="check_redis_status", description="Checks the status of the Redis server."),
    Tool.from_function(func=get_kafka_topics_count, name="get_kafka_topics_count", description="Gets the count of Kafka topics."),
    Tool.from_function(func=get_kafka_groups_count, name="get_kafka_groups_count", description="Gets the count of Kafka consumer groups."),
    Tool.from_function(func=check_topics_groups_match, name="check_topics_groups_match", description="Checks if the number of Kafka topics matches the number of consumer groups."),
    Tool.from_function(func=start_kafka, name="start_kafka", description="Starts the Kafka server."),
    Tool.from_function(func=stop_kafka, name="stop_kafka", description="Stops the Kafka server."),
    Tool.from_function(func=check_kafka_status, name="check_kafka_status", description="Checks the status of the Kafka server."),
    Tool.from_function(func=delete_all_kafka_topics, name="delete_all_kafka_topics", description="Deletes all Kafka topics."),
    Tool.from_function(func=delete_specific_kafka_topic, name="delete_specific_kafka_topic", description="Deletes a specific Kafka topic."),
    Tool.from_function(func=run_create_topics, name="run_create_topics", description="Runs the create_topics.py script to create Kafka topics."),
    Tool.from_function(func=run_validate_topics, name="run_validate_topics", description="Runs the validate_topics.py script to validate Kafka topics."),
    Tool.from_function(func=clear_kafka_directories, name="clear_kafka_directories", description="Clears Kafka and Zookeeper data directories by stopping the services, removing all files from the data directories, and restarting the services."),
    Tool.from_function(func=start_zookeeper, name="start_zookeeper", description="Starts the Zookeeper service."),
    Tool.from_function(func=stop_zookeeper, name="stop_zookeeper", description="Stops the Zookeeper service."),
    Tool.from_function(func=create_panels_file, name="create_panels_file", description="Creates a panels.txt file with one panel per line."),
    Tool.from_function(func=get_panels_file_length, name="get_panels_file_length", description="Gets the number of panels in the panels.txt file."),
    Tool.from_function(func=delete_panels_file, name="delete_panels_file", description="Deletes the panels.txt file from the BASE_DIR."),
    Tool.from_function(func=clean_migration_logs, name="clean_migration_logs", description="Cleans the migration logs directory by backing up existing files to a timestamped directory."),
    Tool.from_function(func=check_migration_processes, name="check_migration_processes", description="Checks if any migration processes are running by checking for: 1. run_producer 2. run_consumer 3. kill_consumer 4. java write process 5. java read process"),
    Tool.from_function(func=kill_migration_processes, name="kill_migration_processes", description="Kills any running migration processes: 1. run_producer 2. run_consumer 3. kill_consumer"),
    Tool.from_function(func=pre_migration_check, name="pre_migration_check", description="Performs pre-migration checks and preparation for migration: 1. Health check 2. Redis cleanup and verification 3. Kafka cleanup, topic creation and validation 4. Log folder cleanup 5. Time series collections validation 6. Push panels to Redis 7. Check for running migration processes 8. Final health check"),
    Tool.from_function(func=start_migration_processes, name="start_migration_processes", description="Starts migration processes and verifies their status and can be used to add more processes or clients to the migration: 1. run_producer.py 2. run_consumer.py 3. kill_consumer.py"),
    Tool.from_function(func=check_migration_concurrency, name="check_migration_concurrency", description="Checks the concurrency of the migration by counting running processes: 1. run_producer.py 2. run_consumer.py"),
    Tool.from_function(func=validate_time_series_collections, name="validate_time_series_collections", description="Validates time series indexes by running ts_mongo_ind_index_validation.py and checks the output log for errors.NOTE: This does not create the time series collections, it only validates them."),
    Tool.from_function(func=start_producer_processes, name="start_producer_processes", description="Starts the run_producer.py script which start the producer processes for all methods."),
    Tool.from_function(func=start_consumer_processes, name="start_consumer_processes", description="Starts the run_consumer.py script which start the consumer processes for all methods."),
    Tool.from_function(func=start_kill_consumer_processes, name="start_kill_consumer_processes", description="Starts the kill_consumer.py script which kills the consumer processes if the migration is completed for the respective method."),
    Tool.from_function(func=push_panels_info_to_redis, name="push_panels_info_to_redis", description="Runs the push_panels_info_to_redis.py script which pushes the panels info to Redis."),
    Tool.from_function(func=run_health_check, name="run_health_check", description="Runs the health_check.py script and returns the result."),
    Tool.from_function(func=read_property_file, name="read_property_file", description="Reads or updates the property file and returns the result."),
    Tool.from_function(func=start_producer_processes_for_specific_methods, name="start_producer_processes_for_specific_methods", description="Starts the run_producer.py script which start the producer processes for specific methods. This function expects a text input with the methods to start the producer for."),
    Tool.from_function(func=start_consumer_processes_for_specific_methods, name="start_consumer_processes_for_specific_methods", description="Starts the run_consumer.py script which start the consumer processes for specific methods. This function expects a text input with the methods to start the consumer for."),
    Tool.from_function(func=backup_redis_data, name="backup_redis_data", description="Takes backup of Redis data in human-readable format for both GET and HGETALL operations."),
    Tool.from_function(func=create_ts_dbs_collections, name="create_ts_dbs_collections", description="Reads the csv containing the panels and cids and creates the time series databases or if databases already exist, it creates the collections."),
    Tool.from_function(func=create_panels_cid_csv_file, name="create_panels_cid_csv_file", description="Creates a csv file containing the panels and cids."),
    
    Tool.from_function(func=get_migrating_panels, name="get_migrating_panels", description="Gets the panels that are currently being migrated."),
    Tool.from_function(func=get_migration_status, name="get_migration_status", description="Get the status of the migration in a table format."),
]

# custom_prompt_template = """"""
memory = ConversationBufferMemory()
prompt = hub.pull("hwchase17/react")
# custom_prompt = PromptTemplate(
#     template=custom_prompt_template
# )
agent = create_react_agent(llm, tools, prompt)
agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True, memory=memory)

def process_smart_query(query):
    """
    Processes a query using the Claude-3 agent.
    
    Args:
        query (str): The query to process
    
    Returns:
        tuple: (success: bool, result: str)
            - success: True if operation was successful, False otherwise
            - result: Response from the agent or error message
    """
    try:
        user_query = query
        output = agent_executor.invoke({"input": user_query})
        return True, output['output']
    except Exception as e:
        return False, f"Failed to process query: {str(e)}"

# API Endpoints
@app.route('/')
def home():
    return jsonify({
        "message": "Welcome to the Redis Management API",
        "status": "success"
    })

# REDIS
@app.route('/redis/start', methods=['POST'])
def api_start_redis():
    success, message = start_redis()
    return jsonify({"success": success, "message": message})

@app.route('/redis/stop', methods=['POST'])
def api_stop_redis():
    success, message = stop_redis()
    return jsonify({"success": success, "message": message})

@app.route('/redis/delete/producer', methods=['DELETE'])
def api_delete_producer_keys():
    success, message = delete_producer_keys()
    return jsonify({"success": success, "message": message})

@app.route('/redis/delete/consumer', methods=['DELETE'])
def api_delete_consumer_keys():
    success, message = delete_consumer_keys()
    return jsonify({"success": success, "message": message})

@app.route('/redis/delete/all', methods=['DELETE'])
def api_delete_all_keys():
    success, message = delete_all_keys()
    return jsonify({"success": success, "message": message})

@app.route('/redis/delete/<key>', methods=['DELETE'])
def api_delete_specific_key(key):
    success, message = delete_specific_key(key)
    return jsonify({"success": success, "message": message})

@app.route('/redis/methods/count', methods=['GET'])
def api_get_methods_count():
    success, result = get_methods_count_form_redis()
    if success:
        return jsonify({"success": True, "data": result})
    return jsonify({"success": False, "message": result})

@app.route('/redis/keys/count', methods=['GET'])
def api_get_total_keys():
    success, result = get_total_keys()
    if success:
        return jsonify({"success": True, "data": result})
    return jsonify({"success": False, "message": result})

@app.route('/redis/status', methods=['GET'])
def api_check_redis_status():
    success, result = check_redis_status()
    if success:
        return jsonify({"success": True, "data": result})
    return jsonify({"success": False, "message": result})

@app.route('/redis/backup', methods=['POST'])
def api_backup_redis():
    success, message = backup_redis_data()
    return jsonify({"success": success, "message": message})

# SLACK 

def message_slack(message):
    """
    Sends a message to the Slack channel using the Slack API.
    """
    print(f"Inside message_slack function")
    if not SLACK_URL:
        print("Error: SLACK_URL environment variable not set.")
        return {
            'statusCode': 500,
            'body': "Error: SLACK_URL environment variable not set."
        }

    slack_data = json.dumps({"text": message}).encode('utf-8')
    req = Request(SLACK_URL, data=slack_data, method='POST')
    req.add_header('Content-Type', 'application/json')
    print(f"Headers Added")
    try:
        print('inside try')
        print('req', req.full_url, req.data, req.headers) # Print more details for debugging
        with urlopen(req) as response:
            print('response', response.status)
            status_code = response.status
            data = response.read().decode('utf-8')
            print(f"Successfully received response from Slack: {data}")
            return {
                'statusCode': status_code,
                'body': data
            }
    except HTTPError as e:
        print(f"HTTP Error calling Slack API: {e.code} {e.reason}")
        return {
            'statusCode': e.code,
            'body': f"HTTP Error: {e.code} {e.reason}"
        }
    except URLError as e:
        print(f"URL Error calling Slack API: {e.reason}")
        return {
            'statusCode': 500,
            'body': f"URL Error: {e.reason}"
        }
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return {
            'statusCode': 500,
            'body': f"Unexpected Error: {e}"
        }

# SMART MIGRATION 
@app.route('/', methods=['POST'])
def api_smart_query():
    """
    Processes a query using the Claude-3 agent.
    
    Request Body:
        {
            "body": "slack json content"
        }
    
    Returns:
        JSON response with the agent's response or error message
    """
    try:
        # Check if this is a duplicate request by checking the timestamp
        current_time = time.time()
        if hasattr(api_smart_query, 'last_request_time') and \
           current_time - api_smart_query.last_request_time < 5:  # 5 second threshold
            return jsonify({
                "success": False,
                "message": "Duplicate request ignored"
            }), 429
        
        api_smart_query.last_request_time = current_time
        
        event = request.get_json()
        body = json.loads(event['body'])
        
        try:
            query = re.sub(r'<[^>]*>', '', body['event']['text'])
        except Exception as e:
            message_slack(f"Invalid request body: {e}")
            return jsonify({
                "success": False,
                "message": "Invalid request body"
            }), 400

        success, result = process_smart_query(query)
        
        message_slack(result)

        if success:
            return jsonify({
                "success": True,
                "data": result
            })
        else:
            return jsonify({
                "success": False,
                "message": result
            })
            
    except Exception as e:
        message_slack(f"Invalid request body: {e}")
        return jsonify({
            "success": False,
            "message": "Invalid request body"
        }), 400

# KAFKA
@app.route('/kafka/topics/count', methods=['GET'])
def api_get_kafka_topics_count():
    success, result = get_kafka_topics_count()
    if success:
        return jsonify({"success": True, "data": result})
    return jsonify({"success": False, "message": result})

@app.route('/kafka/groups/count', methods=['GET'])
def api_get_kafka_groups_count():
    success, result = get_kafka_groups_count()
    if success:
        return jsonify({"success": True, "data": result})
    return jsonify({"success": False, "message": result})

@app.route('/kafka/topics-groups/match', methods=['GET'])
def api_check_topics_groups_match():
    success, result = check_topics_groups_match()
    if success:
        return jsonify({"success": True, "data": result})
    return jsonify({"success": False, "message": result})

@app.route('/kafka/start', methods=['POST'])
def api_start_kafka():
    success, message = start_kafka()
    return jsonify({"success": success, "message": message})

@app.route('/kafka/stop', methods=['POST'])
def api_stop_kafka():
    success, message = stop_kafka()
    return jsonify({"success": success, "message": message})

@app.route('/kafka/status', methods=['GET'])
def api_check_kafka_status():
    success, result = check_kafka_status()
    if success:
        return jsonify({"success": True, "data": result})
    return jsonify({"success": False, "message": result})

@app.route('/kafka/topics/delete/all', methods=['DELETE'])
def api_delete_all_kafka_topics():
    success, message = delete_all_kafka_topics()
    return jsonify({"success": success, "message": message})

@app.route('/kafka/topics/delete/<topic_name>', methods=['DELETE'])
def api_delete_specific_kafka_topic(topic_name):
    success, message = delete_specific_kafka_topic(topic_name)
    return jsonify({"success": success, "message": message})

@app.route('/kafka/topics/create', methods=['POST'])
def api_run_create_topics():
    success, message = run_create_topics()
    return jsonify({"success": success, "message": message})

@app.route('/kafka/topics/validate', methods=['GET'])
def api_run_validate_topics():
    success, message = run_validate_topics()
    return jsonify({"success": success, "message": message})

@app.route('/kafka/clear', methods=['DELETE'])
def api_clear_kafka_directories():
    success, message = clear_kafka_directories()
    return jsonify({"success": success, "message": message})

@app.route('/zookeeper/start', methods=['POST'])
def api_start_zookeeper():
    success, message = start_zookeeper()
    return jsonify({"success": success, "message": message})

@app.route('/zookeeper/stop', methods=['POST'])
def api_stop_zookeeper():
    success, message = stop_zookeeper()
    return jsonify({"success": success, "message": message})

@app.route('/config/read', methods=['GET'])
def api_read_property_file():
    success, result = read_property_file()
    if success:
        return jsonify({"success": True, "data": result})
    return jsonify({"success": False, "message": result})

@app.route('/panels/count', methods=['GET'])
def api_get_panels_file_length():
    success, result = get_panels_file_length()
    if success:
        return jsonify({"success": True, "data": result})
    return jsonify({"success": False, "message": result})

@app.route('/panels/delete', methods=['DELETE'])
def api_delete_panels_file():
    success, message = delete_panels_file()
    return jsonify({"success": success, "message": message})

@app.route('/logs/clean', methods=['POST'])
def api_clean_migration_logs():
    success, message = clean_migration_logs()
    return jsonify({"success": success, "message": message})

@app.route('/migration/processes/status', methods=['GET'])
def api_check_migration_processes():
    success, result = check_migration_processes()
    if success:
        return jsonify({"success": True, "data": result})
    return jsonify({"success": False, "message": result})

@app.route('/migration/processes/kill', methods=['POST'])
def api_kill_migration_processes():
    success, message = kill_migration_processes()
    return jsonify({"success": success, "message": message})

@app.route('/migration/precheck', methods=['POST'])
def api_pre_migration_check():
    success, message = pre_migration_check()
    return jsonify({"success": success, "message": message})

@app.route('/migration/start', methods=['POST'])
def api_start_migration():
    if not request.json or 'process_count' not in request.json:
        return jsonify({
            "success": False,
            "message": "process_count is required in request body"
        }), 400
        
    process_count = request.json['process_count']
    if not isinstance(process_count, int) or process_count < 1:
        return jsonify({
            "success": False,
            "message": "process_count must be a positive integer"
        }), 400
        
    success, message = start_migration_processes()
    return jsonify({"success": success, "message": message})

@app.route('/migration/concurrency', methods=['GET'])
def api_check_migration_concurrency():
    success, result = check_migration_concurrency()
    if success:
        return jsonify({"success": True, "data": result})
    return jsonify({"success": False, "message": result})

@app.route('/migration/validate/ts-indexes', methods=['POST'])
def api_validate_time_series_collections():
    success, message = validate_time_series_collections()
    return jsonify({"success": success, "message": message})

@app.route('/migration/push-panels', methods=['POST'])
def api_push_panels_to_redis():
    success, message = push_panels_info_to_redis()
    return jsonify({"success": success, "message": message})

@app.route('/migration/start/producer', methods=['POST'])
def api_start_producer():
    success, message = start_producer_processes()
    return jsonify({"success": success, "message": message})

@app.route('/migration/start/consumer', methods=['POST'])
def api_start_consumer():
    success, message = start_consumer_processes()
    return jsonify({"success": success, "message": message})

@app.route('/migration/start/kill-consumer', methods=['POST'])
def api_start_kill_consumer():
    success, message = start_kill_consumer_processes()
    return jsonify({"success": success, "message": message})

# Add API endpoint for time series database and collection creation
@app.route('/migration/create/ts-dbs-collections', methods=['POST'])
def api_create_ts_dbs_collections():
    """
    API endpoint to create time series databases and collections from panels_cid.csv
    """
    try:
        # Check if the CSV file exists
        if not os.path.exists(PANELS_CID_CSV_FILE_PATH):
            return jsonify({
                "success": False,
                "message": f"CSV file not found at {PANELS_CID_CSV_FILE_PATH}"
            }), 404

        # Check if the creation script exists
        if not os.path.exists(TS_DBS_CREATION_SCRIPT_PATH):
            return jsonify({
                "success": False,
                "message": f"Time series database creation script not found at {TS_DBS_CREATION_SCRIPT_PATH}"
            }), 404

        # Create the time series databases and collections
        success, message = create_ts_dbs_collections()
        
        if success:
            return jsonify({
                "success": True,
                "message": "Successfully created time series databases and collections",
                "log_file": TS_COLLECTION_CREATION_LOG
            })
        else:
            return jsonify({
                "success": False,
                "message": f"Failed to create time series databases and collections: {message}"
            }), 500

    except Exception as e:
        return jsonify({
            "success": False,
            "message": f"An error occurred: {str(e)}"
        }), 500

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=9001, use_reloader=False)

