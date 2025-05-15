import redis
import json
import time
import subprocess
import re
import logging
from datetime import datetime
import sys
import psutil

TIME_GAP_BETWEEN_CHECKS_SECS = 2 # 5*60

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

KAFKA_BROKER = config_dict['kafka_bootstrap_servers']

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

def get_kafka_offsets(group_name, bootstrap_server="172.31.23.48:9092"):
    try:
        cmd = f"/usr/local/kafka_2.13-2.6.2/bin/kafka-consumer-groups.sh --bootstrap-server {bootstrap_server} --group {group_name} --describe"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

        if result.returncode != 0:
            log_message('ERROR', {'msg': 'Failed to run command', 'command': result.stderr.strip()})
            return None, None

        lines = result.stdout.split("\n")
        current_offsets = {}
        log_end_offsets = {}

        regex_pattern = re.compile(rf"{group_name}\s+(\S+)\s+(\d+)\s+(\d+)\s+(\d+)\s+\d+")

        for line in lines:
            match = regex_pattern.search(line)
            if match:
                topic, partition, current_offset, log_end_offset = match.groups()
                partition = int(partition)
                current_offsets[partition] = int(current_offset)
                log_end_offsets[partition] = int(log_end_offset)

        if not current_offsets or not log_end_offsets:
            log_message('WARNING', {'msg': 'No offsets found.', 'group_name': group_name})
            return None, None

        return current_offsets, log_end_offsets

    except Exception as e:
        log_message('ERROR', {'msg': 'Error occurred while getting kafka offsets', 'err': str(e)})
        return None, None

def get_is_logend_current_offsets_same(end_offsets, consumer_offsets):
    if end_offsets == consumer_offsets:
        return True
    return False


def check_consumer_movement(topic_name):
    consumer_group = f"{topic_name}_grp"
    redis_client = redis.Redis(host=redis_config['redis_host'], port=redis_config['redis_port'], db=redis_config['redis_db'])
    
    redis_key = f"kafka_offset_tracking:{topic_name}"
    
    end_offsets, consumer_offsets = get_kafka_offsets(consumer_group, KAFKA_BROKER)
    if end_offsets is None or consumer_offsets is None:
        log_message('WARNING', {'msg': 'Unable to retrieve offsets, skipping...', 'topic': topic_name})
        return True

    prev_data = redis_client.get(redis_key)
    
    if prev_data is None:
        redis_client.set(redis_key, json.dumps({"end": end_offsets, "consumer": consumer_offsets}))
        log_message("INFO", {'msg': 'First-time tracking and considered as moving.', 'topic': topic_name})
        return True

    prev_data = json.loads(prev_data)
    prev_data["end"] = {int(k): int(v) for k, v in prev_data["end"].items()}
    prev_data["consumer"] = {int(k): int(v) for k, v in prev_data["consumer"].items()}
    
    if prev_data["end"] != end_offsets or prev_data["consumer"] != consumer_offsets:
        log_message('INFO', {'msg': 'Consumer is moving, updating Redis', 'topic': topic_name})
        redis_client.set(redis_key, json.dumps({"end": end_offsets, "consumer": consumer_offsets}))
        return True
    else:
        is_logend_current_offsets_same = get_is_logend_current_offsets_same(end_offsets, consumer_offsets)
        if is_logend_current_offsets_same:
            log_message('INFO', {'msg':'current offset reached log end offset', 'topic': topic_name})
            return False
        else:
            log_message('INFO', {'msg': 'logend offset not moving, but consumer offset is moving', 'topic':topic_name})
            return True


def get_is_status_completed(redis_host, redis_port, redis_db, key, field):
    try:
        redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db, decode_responses=True)

        value = redis_client.hget(key, field)
        if not value:
            log_message('ERROR', {'msg': f'No data found for key: {key}', field: {field}})
            return False

        data = json.loads(value)

        if isinstance(data, list):
            return (data[0].get("status") == "completed" or data[0].get("status") == "killed")

        return (data.get("status") == "completed" or data.get("status") == "killed")

    except Exception as e:
        print(f"[ERROR] {e}")
        return False


def check_if_process_exists(pid):
    try:
        return psutil.pid_exists(int(pid))
    except Exception as e:
        log_message('ERROR', {'err': e})
        return False

    
producer_consumer_methods_map = {
    "readUserAttributes": "writeUserAttributes",
    "readAnonUserAttributes": "writeAnonUserAttributes",
    "readDisableUserAttributes": "writeDisableUserAttributes",
    "readEngagementEventsWithMetaKey": "writeEngagementEventsToUserEvents",
    "readAnonEngagementEventsWithMetaKey": "writeAnonEngagementEventsToAnonUserEvents",
    "readDisableEngagementEventsWithMetaKey": "writeDisableEngagementEventsToDisabledUserEvents",
    "readUserDetailsWithMetaKey": "writeUserDetailsToUserEvents",
    "readAnonUserDetailsWithMetaKey": "writeAnonUserDetailsToAnonUserEvents",
    "readDisableUserDetailsWithMetaKey": "writeDisableUserDetailsToDisableUserEvents"
}

consumer_producer_methods_map = {
    "writeUserAttributes": "readUserAttributes",
    "writeAnonUserAttributes": "readAnonUserAttributes",
    "writeDisableUserAttributes": "readDisableUserAttributes",
    "writeEngagementEventsToUserEvents": "readEngagementEventsWithMetaKey",
    "writeAnonEngagementEventsToAnonUserEvents": "readAnonEngagementEventsWithMetaKey",
    "writeDisableEngagementEventsToDisabledUserEvents": "readDisableEngagementEventsWithMetaKey",
    "writeUserDetailsToUserEvents": "readUserDetailsWithMetaKey",
    "writeAnonUserDetailsToAnonUserEvents": "readAnonUserDetailsWithMetaKey",
    "writeDisableUserDetailsToDisableUserEvents": "readDisableUserDetailsWithMetaKey"
}

def run_command(command):
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        return result.stdout.strip()
    except Exception as e:
        return str(e)

def parse_log_line(log_line):
    try:
        match = re.search(r'Consumed:\s*(\d+)?\s*Produced:\s*(\d+)?', log_line)
        if match:
            consumed = int(match.group(1)) if match.group(1) else None
            produced = int(match.group(2)) if match.group(2) else None
            return consumed, produced
        return None, None
    except Exception as e:
        log_message('ERROR', {'msg': f"Error occurred while parsing log line: {log_line}", 'err': str(e)})
        return None, None
    
def check_produced_eq_conumed(group_name):
    command = """/usr/local/kafka_2.13-2.6.2/bin/kafka-consumer-groups.sh --describe --group """ + group_name + """ --bootstrap-server """ + KAFKA_BROKER + """ | awk -F " " '($4 != "-" && $4 >= 0 && $5 != "-" && $5 >= 0 && $6 != "-" && $6 >= 0){count1 += $4;count2 +=$5;count3 +=$6;d=strftime("%Y-%m-%d %H:%M:%S");} END {print d,"Consumed:",count1, "Produced:",count2,"Lag:",count3;}'"""
    output = run_command(command)
    produced, consumed = parse_log_line(output)
    if produced is None or consumed is None:
        log_message('Info', {'msg': f"produced: {produced} consumed: {consumed}", "group": group_name})
        return False
    elif produced is not None and consumed is not None:
        log_message('Info', {'msg': f"produced: {produced} consumed: {consumed}", "group": group_name})
        return consumed == produced
    else:
        log_message('ERROR', {'msg': "unable to parse log line", "group": group_name})
        return False

if __name__ == "__main__":   
    if len(sys.argv) == 3:
        log_file_name = sys.argv[1]
        current_env = sys.argv[2]
    elif len(sys.argv) == 4:
        panels_file_path = sys.argv[1]
        log_file_name = sys.argv[2]
        current_env = sys.argv[3]
    else:
        print('usage: python3 orchestrate.py [<panels_file_path>] <log_file_path> <current_env>')
        exit()

    setup_logger(log_file_name)
    
    while True:
        try:
            redis_client = redis.Redis(host=redis_config['redis_host'], port=redis_config['redis_port'], db=redis_config['redis_db'])
            if len(sys.argv) == 2:
                keys = redis_client.scan_iter(match="consumer*")
            else:
                with open(panels_file_path, 'r') as f:
                    panels = [panel.strip() for panel in f]
                
                keys = set()
                for panel in panels:
                    consumer_redis_key = "consumer_" + panel
                    keys.add(consumer_redis_key)
        except Exception as e:
            log_message('ERROR', {'msg': "error scanning consumer", 'err': e})

        for key in keys:
            if isinstance(key, bytes):
                consumer_redis_key = key.decode('utf-8')
            else:
                consumer_redis_key = key
                
            try:
                fields = redis_client.hkeys(consumer_redis_key)
            except Exception as e:
                log_message('ERROR', {'msg': "error getting fields from consumer", 'key': consumer_redis_key, 'err': e})

            consumer_fields = [field.decode('utf-8') for field in fields]

            for consumer_field in consumer_fields:
                try:
                    data = redis_client.hget(consumer_redis_key, consumer_field).decode('utf-8')
                except Exception as e:
                    log_message('ERROR', {'msg': "error getting data from consumer", 'key':consumer_redis_key, 'field': consumer_field, 'err': e})

                data = json.loads(data)
                # if data['env'] != current_env:
                #     continue
                
                status = data['status']
                consumer_pid = data['pid']
                env = data['env']

                if status == 'running':
                    topic_name = data['topic_name']
                    group_name = data['group_name']

                    client = consumer_redis_key.removeprefix("consumer_")
                    producer_redis_key = "producer_" + client
                    
                    producer_redis_field = consumer_producer_methods_map.get(consumer_field)

                    is_consumer_moving = check_consumer_movement(topic_name)
                    
                    is_status_completed = get_is_status_completed(redis_config['redis_host'], redis_config['redis_port'], redis_config['redis_db'], producer_redis_key, producer_redis_field)
                    if is_status_completed:
                        if check_produced_eq_conumed(group_name): # not is_consumer_moving and
                            if consumer_pid:
                                log_message('INFO', {'msg': 'killing consumer', 'consumer_pid': consumer_pid, 'topic_name': topic_name})
                                subprocess.run(f"kill -15 {consumer_pid}", shell=True)
                                time_sec = 0
                                while check_if_process_exists(consumer_pid):
                                    time.sleep(2)
                                    time_sec += 2
                                    if time_sec >= 60: # TIME_SEC_BEFORE_KILL
                                        log_message('ERROR', {'msg': 'consumer did not stop after 60 seconds after issuing the kill', 'consumer_pid': consumer_pid, 'topic_name': topic_name})
                                        subprocess.run(f"kill -9 {consumer_pid}", shell=True)
                                        break
                                # update the status
                            data_to_update = redis_client.hget("consumer_" + client, consumer_field)
                            data_to_update = json.loads(data_to_update)
                            if isinstance(data, list):
                                data = data[0]
                            data['status'] = 'completed'
                            redis_client.hset(consumer_redis_key, consumer_field, json.dumps(data))
                        else:
                            continue 
                    else:
                        continue
        
        time.sleep(TIME_GAP_BETWEEN_CHECKS_SECS)
