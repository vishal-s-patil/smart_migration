import ast
import redis
import subprocess
import psutil
import time
import threading
import logging
from datetime import datetime
import random
import json
import sys
import signal
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

redis_config = {
    "redis_host": config_dict['redis_uri'],
    "redis_port": int(config_dict['redis_port']),
    "redis_db": int(config_dict.get('redis_db', 0)) 
}

SLEEP_TIME_BEFORE_FETCHING_PID_SEC = 3
SLEEP_TIME_BEFORE_CHECKING_PROCESS_STATUS_SEC = 2

queue_pid_map = {
    "writeUserAttributes" : None, 
    "writeAnonUserAttributes" : None, 
    "writeDisableUserAttributes": None,
    "writeEngagementEventsToUserEvents" : None, 
    "writeAnonEngagementEventsToAnonUserEvents" : None, 
    "writeDisableEngagementEventsToDisabledUserEvents" : None, 
    "writeUserDetailsToUserEvents" : None, 
    "writeAnonUserDetailsToAnonUserEvents": None, 
    "writeDisableUserDetailsToDisableUserEvents": None
}
 

def handle_sigterm(signum, frame):
    log_message("INFO", {"msg": f"Received SIGTERM, Killing producers and producer orchestration itself."})
    for method, pid in queue_pid_map.items():
        if pid is not None:
            try:
                subprocess.run(["kill", "-15", str(pid)])
                log_message('INFO', {"msg": f"Killed producer for method: {method}", "method": method, "pid": pid})
            except Exception as e:
                log_message('ERROR', {"msg": f"Error while killing producer for method: {method}", "method": method, "pid": pid, "error": e})
        else:
            log_message('WARNING', {"msg": f"No PID found for method: {method}"})
    sys.exit(0)

signal.signal(signal.SIGTERM, handle_sigterm)

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

try:
    r = redis.Redis(host=redis_config['redis_host'], port=redis_config['redis_port'], db=redis_config['redis_db'], decode_responses=True)
except Exception as e:
    log_message('ERROR', {"msg": "error while connecting to redis", "error": e})
    exit()


def run_command_get_pid(command):
    """Run a command and return the process object."""
    process = subprocess.Popen(command, shell=True)
    log_message("INFO", {"msg": "Process started successfully", "command": command})
    return process


def is_process_running(pid):
    """Check if a process with the given PID is running."""
    running = psutil.pid_exists(int(pid))
    log_message('INFO', {"message": f"Checking if PID {pid} is running", "is_process_running": running})
    return running

# def wait_for_process_to_complete(pid, panel_name, method):
#     try:
#         process = psutil.Process(pid)
#         while process.is_running():
#             time.sleep(SLEEP_TIME_BEFORE_CHECKING_PROCESS_STATUS_SEC)
#         return
#     except psutil.NoSuchProcess:
#         # log_message('ERROR', {'msg': 'No process found', 'pid': pid, "client": panel_name, "method": method})
#         return
#     except Exception as e:
#         # log_message('ERROR', {'msg': "Error while wating for process: ", "pid": pid, "client": panel_name, "method": method})
#         return


def wait_for_process_to_complete(pid, panel_name, method, check_interval=1):
    while True:
        result = subprocess.run(["ps", str(pid)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if str(pid) not in result.stdout.decode():
            log_message('INFO', {'msg': 'No process found', 'pid': pid, "client": panel_name, "method": method})
            return
        time.sleep(check_interval)


def run_migration(redis_client, redis_key, custom_property_file):
    redis_key = redis_key + "_queue"
    while True:
        panel_data = redis_client.lpop(redis_key)
        if panel_data:
            panel_data = ast.literal_eval(panel_data)  
            if custom_property_file:
                command = "/home/smartechro/mongo_migration_custom_propertyfile.sh "
            else:
                command = "/home/smartechro/mongo_migration.sh "
            command += redis_key.removesuffix("_queue")
            panel_name = panel_data.get('panel_name')
            command += f" {panel_name}" 
            
            log_message('INFO', {'msg': "starting command", "command": {command}})
            if custom_property_file:
                command += " " + custom_property_file
            process = run_command_get_pid(command)
            
            time.sleep(SLEEP_TIME_BEFORE_FETCHING_PID_SEC)
            
            search_key = "consumer_" + panel_name
            search_method = redis_key.removesuffix("_queue")
            data = redis_client.hget(search_key, search_method)

            MAX_TRIES_FOR_FINDING_KEY = 60
            retry = 1
            flag = 1
            while data is None:
                retry += 1
                data = redis_client.hget(search_key, search_method)
                if data is None:
                    log_message('WARNING', {"msg": "No data found for key", "redis_key": search_key, "search_field": search_method, "retry": retry})
                    time.sleep(1)
                if retry >= MAX_TRIES_FOR_FINDING_KEY:
                    log_message('ERROR', {"msg": "failed to get data", "redis_key": search_key, "search_field": search_method})
                    flag = 2
                    break

            if flag == 2:
                continue

            data = json.loads(data)
            if isinstance(data, list):
                data = data[0]
            pid = data["pid"]

            queue_pid_map[redis_key.removesuffix("_queue")] = pid

            log_message('INFO', {"msg": "Waiting for process to complete", 'pid': pid, "client": panel_name, "method": redis_key.removesuffix("_queue")})
            wait_for_process_to_complete(pid, panel_name, redis_key.removesuffix("_queue"))
            log_message('INFO', {"msg": "process completed", 'pid': pid, "client": panel_name, "method": redis_key.removesuffix("_queue")})
            time.sleep(1)
        else:
            log_message('INFO', {"msg": "No more data in redis", "redis_key": redis_key})
            return None


def run_migration_all(redis_client, redis_keys, custom_property_file):
    """Starts independent threads for each Redis key to handle processes concurrently."""
    threads = []
    for redis_key in redis_keys:
        thread = threading.Thread(target=run_migration, args=(redis_client, redis_key, custom_property_file), daemon=True)
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()  

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the producer script with logging and optional methods.")
    parser.add_argument("log_file_name", help="Name of the log file")
    parser.add_argument("--methods", help="Comma-separated list of methods", default="")
    parser.add_argument("--custom-property-file", help="Path to the custom property file", default=None)
    
    args = parser.parse_args()

    log_file_name = args.log_file_name
    custom_property_file = args.custom_property_file
    consumer_methods = args.methods.split(",") if args.methods else ["writeUserAttributes", "writeAnonUserAttributes", "writeDisableUserAttributes", "writeEngagementEventsToUserEvents", "writeAnonEngagementEventsToAnonUserEvents", "writeDisableEngagementEventsToDisabledUserEvents", "writeUserDetailsToUserEvents", "writeAnonUserDetailsToAnonUserEvents", "writeDisableUserDetailsToDisableUserEvents"]


    # if len(sys.argv) == 2:
    #     log_file_name = sys.argv[1]
    # else:
    #     print('run: python3 run_producer <log_file_name>')
    setup_logger(log_file_name)
    run_migration_all(r, consumer_methods, custom_property_file)
