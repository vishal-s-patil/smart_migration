import subprocess
import logging
import re 
import redis
import json
from datetime import datetime

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

def get_completed_group_names(redis_host, redis_port, key):
    try:
        r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        data = r.hgetall(key)
        group_names = []

        for filed, dt in data.items():
            dt = json.loads(dt)
            if dt['status'] == "completed":
                group_names.append(dt['group_name'])
        return group_names
    except Exception as e:
        print('error', e)
        return []


def read_lines_from_file(file_path):
    try:
        with open(file_path, "r") as f:
            return [line.strip() for line in f]
    except Exception as e:
        return str(e)

# Example usage
# redis_host = "127.0.0.1"
# redis_port = 6379
# key = "consumer_fundexpertfintechcee"
# print(get_completed_group_names(redis_host, redis_port, key))

def run_command(command):
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        return result.stdout.strip()
    except Exception as e:
        return str(e)
    

if __name__ == "__main__":
    panels = read_lines_from_file('panels.txt')
    completed_group_names = []
    for panel in panels:
        key = "consumer_" + str(panel)
        group_names = get_completed_group_names("127.0.0.1", 6379, key)
        # for group_name in group_names:
        #     if group_name[-3:] != 'grp':
        #         group_names.remove(group_name)
        completed_group_names.extend(group_names)
    # print(completed_group_names)    
    print("total_completed_groups:", len(completed_group_names))
    for group_name in completed_group_names:
        command = """/usr/local/kafka_2.13-2.6.2/bin/kafka-consumer-groups.sh --describe --group """ + group_name + """ --bootstrap-server 172.31.23.48:9092 | awk -F " " '($4 != "-" && $4 >= 0 && $5 != "-" && $5 >= 0 && $6 != "-" && $6 >= 0){count1 += $4;count2 +=$5;count3 +=$6;d=strftime("%Y-%m-%d %H:%M:%S");} END {print d,"Consumed:",count1, "Produced:",count2,"Lag:",count3;}'"""

        output = run_command(command)
        print(group_name, ": ", output)
