import requests
import json
import psutil
import logging
import redis
import argparse

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

REDIS_HOST = props.get("redis_uri", "localhost")
REDIS_PORT = int(props.get("redis_port", 6379))

SLACK_WEBHOOK_URL = props.get("monitoring_slack_url", "")

def send_slack_alert(message, pid):
    if not SLACK_WEBHOOK_URL:
        # logger.warning("Slack webhook URL is not configured. Alert not sent.")    
        return
    
    payload = {
        "text": (
            f"<!channel> \n"
            f"📍 *Message:* `{message}` \n"
            f"📍 *PID:* `{pid}` \n"
        )
    }
    response = requests.post(SLACK_WEBHOOK_URL, data=json.dumps(payload), headers={"Content-Type": "application/json"})


def is_process_running(pid):
    try:
        result = psutil.pid_exists(pid)
        # logger.debug(f"Process {pid} running: {result}")
        return result
    except Exception as e:
        # logger.error(f"Error checking process {pid}: {e}")    
        return False


def parse_args():
    parser = argparse.ArgumentParser(description="Parse command-line arguments.")
    parser.add_argument("numbers", type=str, help="Comma-separated list of numbers")
    parser.add_argument("logfilename", type=str, nargs="?", help="Log file name", default="/var/log/apps/mongodataremodel/orchestartion_monitoring.log")
    args = parser.parse_args()

    number_list = list(map(int, args.numbers.split(',')))
    return number_list, args.logfilename

if __name__ == "__main__":
    conn = redis.Redis(host="localhost", port=REDIS_PORT, db=0)
    pids, logfilename = parse_args()

    logging.basicConfig(filename=logfilename, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    for pid in pids:
        if is_process_running(pid):
            logger.info(f"Info: PID {pid} is running")
            # send_slack_alert(f"Panel '{panel_name}' with PID {pid} is still running", pid)
        else:
            logger.info(f"Warn: PID {pid} is not running")
            send_slack_alert(f"WARNING: Process with PID {pid} has stopped, remove from the list if finished.", pid)
