import redis
import sys
import ast
import subprocess
import logging
from datetime import datetime

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

#mongosh -u root -p icyiguana30  -host nc-mgin3-cfg2.netcore.in --port 27015 --authenticationDatabase admin
# should be mogno5
mongo_config = parse_mongo_uri(config_dict['src_mongo_uri'])

redis_config = {
    "redis_host": config_dict['redis_uri'],
    "redis_port": int(config_dict['redis_port']),
    "redis_db": int(config_dict.get('redis_db', 0)) 
}

# get_latest_max_uids_commands="""perl -lne 'chomp; $cmd="mongosh -u root -p icyiguana30  -host nc-mgin3-cfg2.netcore.in --port 27015 --authenticationDatabase admin $_ --eval=\\047db.userDetails.find({},{uid:1,_id:0}).sort({uid:-1}).limit(1)\\047" if $_; $panel=$_ if $_; $out=`$cmd`; $uid = ($out =~ /uid: (\d+)/) ? $1 : ""; print "$panel,$uid"' """ + f"""{csv_file_path}"""
# mongo_config = {
#     "host": "nc-mgin3-cfg2.netcore.in",
#     "port": 27015,
#     "user": "root",
#     "passwd": "icyiguana30"
# }

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
    print(f'Error while connecting to redis: %s' % e)
    exit()

producer_methods = ["readUserAttributes", "readAnonUserAttributes", "readDisableUserAttributes", "readEngagementEventsWithMetaKey", "readAnonEngagementEventsWithMetaKey", "readDisableEngagementEventsWithMetaKey", "readUserDetailsWithMetaKey", "readAnonUserDetailsWithMetaKey", "readDisableUserDetailsWithMetaKey"]
consumer_methods = ["writeUserAttributes", "writeAnonUserAttributes", "writeDisableUserAttributes", "writeEngagementEventsToUserEvents", "writeAnonEngagementEventsToAnonUserEvents", "writeDisableEngagementEventsToDisabledUserEvents", "writeUserDetailsToUserEvents", "writeAnonUserDetailsToAnonUserEvents", "writeDisableUserDetailsToDisableUserEvents"]

# producer_methods = ["readAnonUserAttributes", "readDisableUserAttributes", "readAnonEngagementEventsWithMetaKey", "readDisableEngagementEventsWithMetaKey", "readAnonUserDetailsWithMetaKey", "readDisableUserDetailsWithMetaKey"]
# consumer_methods = ["writeAnonUserAttributes", "writeDisableUserAttributes", "writeAnonEngagementEventsToAnonUserEvents", "writeDisableEngagementEventsToDisabledUserEvents", "writeAnonUserDetailsToAnonUserEvents", "writeDisableUserDetailsToDisableUserEvents"]


def push_panel_to_redis(clients, is_both):
    try:
        cnt = 0
        for client, max_uid in clients:
            try:
                panel_name = client
                start_uid = 1
                if max_uid is not None:
                    end_uid = int(max_uid + max_uid * 0.1)
                    continue
                    
                # start_uid = int(start_uid) if start_uid.isdigit() else None
                # end_uid = int(end_uid) if end_uid.isdigit() else None

                panel_data = {"panel_name": panel_name}
                if start_uid is not None:
                    panel_data["start_uid"] = start_uid
                if end_uid is not None:
                    panel_data["end_uid"] = end_uid
                
                if is_both == 1:
                    for producer_method in producer_methods:
                        # print(producer_method + "_queue", str(panel_data))
                        r.rpush(producer_method + "_queue", str(panel_data))
                
                    for consumer_method in consumer_methods:
                        # print(consumer_method + "_queue", str(panel_data))
                        r.rpush(consumer_method + "_queue", str(panel_data))
                    log_message("INFO", {"db": client, "msg": f"successfully pushed"})
                elif is_both == 2:
                    for producer_method in producer_methods:
                        # print(producer_method + "_queue", str(panel_data))
                        r.rpush(producer_method + "_queue", str(panel_data))
                else:
                    for consumer_method in consumer_methods:
                        # print(consumer_method + "_queue", str(panel_data))
                        r.rpush(consumer_method + "_queue", str(panel_data))
                cnt += 1
            except Exception as e:
                log_message("ERROR", {"db": client, "msg": "Error while pushing panel to redis", "err": e})
                continue

        log_message("INFO", {"msg": f"pushed {cnt} panels to redis"})
    except Exception as e:
        print(f"Error while pusing panels info to redis : {e}")
        log_message("ERROR", {"msg": "Error while pusing panels info to redis", "err": e})


def read_panel_from_redis(redis_client, redis_key):
    try:
        panel_data = redis_client.lpop(redis_key)
        if panel_data:
            panel_data = ast.literal_eval(panel_data)  # Convert string back to dictionary
            # print(f"Retrieved: {panel_data}")
            return panel_data
        else:
            print("No more data in Redis.")
            return None
    except Exception as e:
        print(f"Error: {e}")
        return None


def run_shell_command(command):
    try:
        result = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return result.stdout.decode("utf-8")
    except subprocess.CalledProcessError as e:
        raise Exception(f"Shell command error: {e}")
    except subprocess.SubprocessError as e:
        raise Exception(f"Shell subprocess error: {e}")


if __name__ == "__main__":
    if len(sys.argv) == 4:
        csv_file_name = sys.argv[1]
        log_file_name = sys.argv[2]
        is_both = int(sys.argv[3])

        setup_logger(log_file_name)

        get_latest_max_uids_commands="""perl -lne 'chomp; $cmd="mongosh -u """ + mongo_config['user'] + """ -p """ + mongo_config['passwd'] + """ -host """ + mongo_config["host"] + """ --port """ + str(mongo_config['port']) + """ --authenticationDatabase admin $_ --eval=\\047db.userDetails.find({},{uid:1,_id:0}).sort({uid:-1}).limit(1)\\047" if $_; $panel=$_ if $_; $out=`$cmd`; $uid = ($out =~ /uid: (\d+)/) ? $1 : ""; print "$panel,$uid"' """ + f"""{csv_file_name}"""
        
        try:
            clients = run_shell_command(get_latest_max_uids_commands)
            clients = [line.strip().split(",") for line in clients.strip().split("\n")]
        except Exception as e:
            log_message("ERROR", {"mag": "Error executing shell command", "command": get_latest_max_uids_commands, "error": str(e)})
            exit()

        clients = [[name, int(num) if num.isdigit() else None] for name, num in clients]
        for name, num in clients:
            if num is None:
                log_message("ERROR", {"mag": "client not found", "client": name})
                clients.remove([name, num])
                continue

        clients = [client for client in clients if client[1] is not None]
        
        log_message("INFO", {"msg": f"starting to push {len(clients)} panels to redis"})
        push_panel_to_redis(clients, is_both)

        # for producer_method in producer_methods: # consumer_methods:
        #     producer_method = producer_method + "_queue"
        #     data = read_panel_from_redis(r, producer_method)            
        #     while data:
        #         print(data)
        #         data = read_panel_from_redis(r, producer_method)
        #     print()

    else:
        print("Please provide a CSV file path and redis key as an argument.")
