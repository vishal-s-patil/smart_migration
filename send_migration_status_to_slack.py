import shutil
import redis
import json
import logging
import requests
import sys
import subprocess

# Load properties
def load_properties(path):
    props = {}
    with open(path, 'r') as f:
        for line in f:
            if '=' in line:
                key, value = line.strip().split('=', 1)
                props[key.strip()] = value.strip()
    return props

# Load config
PROPERTIES_FILE = "/etc/mongoremodel.properties"
props = load_properties(PROPERTIES_FILE)

REDIS_HOST = props.get("redis_uri", "localhost")
REDIS_PORT = int(props.get("redis_port", 6379))
SLACK_WEBHOOK_URL = props.get("monitoring_slack_url", "")
ENV = props.get("env", "unknown")
KAFKA_SCRIPT_PATH = "/home/mongodb/smart_migration/tabulate_data.py"

# Logging
LOG_FILE = "/var/log/apps/mongodataremodel/migration_monitoring.log"
ENABLE_LOGGING = True
logging.basicConfig(filename=LOG_FILE, level=logging.DEBUG if ENABLE_LOGGING else logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Redis connection
redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

panels_file_name = sys.argv[2]


def get_disk_utilization():
    """
    Gets the disk utilization percentage for the root (/) and /database partitions.

    Returns:
        tuple: A tuple containing the disk utilization percentage for root
               and /database (float, float). Returns (None, None) if a
               partition cannot be found.
    """
    root_usage = None
    database_usage = None

    try:
        root_disk = shutil.disk_usage("/")
        root_total = root_disk.total
        root_used = root_disk.used
        if root_total > 0:
            root_usage = (root_used / root_total) * 100
    except FileNotFoundError:
        print("Warning: Root partition '/' not found.")
    except Exception as e:
        print(f"Error getting root disk usage: {e}")

    try:
        database_disk = shutil.disk_usage("/data")
        database_total = database_disk.total
        database_used = database_disk.used
        if database_total > 0:
            database_usage = (database_used / database_total) * 100
    except FileNotFoundError:
        print("Warning: /data partition not found.")
    except Exception as e:
        print(f"Error getting /data disk usage: {e}")

    return (round(root_usage, 2), round(database_usage, 2))

# Slack alert
def send_slack_alert(message):
    if not SLACK_WEBHOOK_URL:
        logger.warning("Slack webhook URL is not configured. Alert not sent.")
        return

    payload = {"text": message}
    logger.debug(f"Sending Slack alert: {json.dumps(payload)}")
    try:
        response = requests.post(SLACK_WEBHOOK_URL, data=json.dumps(payload),
                                 headers={"Content-Type": "application/json"})
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Failed to send Slack alert: {e}")

def get_kafka_status():
    """
    Runs the Kafka status script and returns the output.
    """
    try:
        subprocess.run(["chmod", "+x", KAFKA_SCRIPT_PATH], check=True)
        result = subprocess.run(["python3", KAFKA_SCRIPT_PATH, "/home/mongodb/smart_migration/" + panels_file_name],
                                capture_output=True, text=True, check=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        logger.error(f"Error running Kafka status script: {e}")
        return f"Error running Kafka status script: {e.stderr}"
    except FileNotFoundError:
        logger.error(f"Kafka status script not found at {KAFKA_SCRIPT_PATH}")
        return f"Error: Kafka status script not found at {KAFKA_SCRIPT_PATH}"
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        return f"An unexpected error occurred: {e}"

# Main
if __name__ == "__main__":
    total_cnt = sys.argv[1]

    queue_list = [
        "readUserDetailsWithMetaKey_queue",
        "readUserAttributes_queue",
        "readDisableEngagementEventsWithMetaKey_queue",
        "readAnonEngagementEventsWithMetaKey_queue",
        "readDisableUserDetailsWithMetaKey_queue",
        "readDisableUserAttributes_queue",
        "readAnonUserDetailsWithMetaKey_queue",
        "readEngagementEventsWithMetaKey_queue",
        "readAnonUserAttributes_queue",
        "writeUserDetailsToUserEvents_queue",
        "writeUserAttributes_queue",
        "writeDisableEngagementEventsToDisabledUserEvents_queue",
        "writeAnonEngagementEventsToAnonUserEvents_queue",
        "writeDisableUserDetailsToDisableUserEvents_queue",
        "writeDisableUserAttributes_queue",
        "writeAnonUserDetailsToAnonUserEvents_queue",
        "writeEngagementEventsToUserEvents_queue",
        "writeAnonUserAttributes_queue",
    ]

    total_pending_count = 0
    redis_status_lines = [f"Env: {ENV}"]
    redis_status_lines.append("+---------------------------------------------------------+--------------------+")
    redis_status_lines.append("| {:<55} | {:<16} |".format("Queue Name", "Pending / Total"))
    redis_status_lines.append("+---------------------------------------------------------+--------------------+")

    for queue in queue_list:
        try:
            qlen = redis_client.llen(queue)
            total_pending_count += qlen
            redis_status_lines.append("| {:<55} | {} / {:<16} |".format(queue, qlen, total_cnt))
        except Exception as e:
            logger.error(f"Error accessing queue {queue}: {e}")
            redis_status_lines.append("| {:<55} | Error reading            |".format(queue))

    redis_status_lines.append("+---------------------------------------------------------+--------------------+")
    redis_message = "Redis Status:\n" + "\n".join([f"`{line}`" for line in redis_status_lines])

    kafka_status_output = get_kafka_status()
    message = ""

    if total_pending_count > 0:
        message += redis_message + "\n"
    else:
        logger.info("All queue lengths are zero.")

    if "No data found" not in kafka_status_output:
        message += "\nKafka Status:\n"
        message += "\n".join([f"`{line}`" for line in kafka_status_output.splitlines()])

        message += "\n\nDisk space utilisation"
        disk_space_util = get_disk_utilization()
        message += "\nroot partition (/): " + str(disk_space_util[0])
        message += "\ndata partition (/data): " + str(disk_space_util[1])
    send_slack_alert(message)

