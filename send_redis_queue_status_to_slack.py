import redis
import json
import logging
import requests
import sys

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

# Logging
LOG_FILE = "/var/log/apps/mongodataremodel/migration_monitoring.log"
ENABLE_LOGGING = True
logging.basicConfig(filename=LOG_FILE, level=logging.DEBUG if ENABLE_LOGGING else logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Redis connection
redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

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

    table_header = f"Env: {ENV}\n"
    table_header += "+---------------------------------------------------------+--------------------+\n"
    table_header += "|                     Queue Name                          | Pending Count      |\n"
    table_header += "+---------------------------------------------------------+--------------------+\n"

    table_rows = ""
    for queue in queue_list:
        try:
            qlen = redis_client.llen(queue)
            total_pending_count += qlen
            table_rows += f"| {queue:<55} | {qlen} / {total_cnt:<16} |\n"
        except Exception as e:
            logger.error(f"Error accessing queue {queue}: {e}")
            table_rows += f"| {queue:<55} | Error reading        |\n"

    # Only send if there's at least one non-zero queue
    if total_pending_count > 0:
        table_footer = "+---------------------------------------------------------+--------------------+"
        full_message = f"```{table_header}{table_rows}{table_footer}```"
        send_slack_alert(full_message)
    else:
        logger.info("All queue lengths are zero. Slack alert not sent.")
