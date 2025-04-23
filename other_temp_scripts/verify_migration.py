import argparse
import logging
import os
import subprocess
import sys
from datetime import datetime
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import time

src_mongo_config = {
    "user": "root",
    "passwd": "icyiguana30",
    "host": "nc-mgin3-cfg2.netcore.in",
    "port": 27015,
    "authdb": "admin"
}

dest_mongo_config = {
    "user": "root",
    "passwd": "icyiguana30",
    "host": "15.207.170.84",
    "port": 27015,
    "authdb": "admin"
}

NUMBER_OF_PARTITIONS = 10
EVENTS_PADDING = 0.05

method_map = {
    'DisableUserAttributes': 'writeDisableUserAttributes',
    'AnonUserEvents': 'writeAnonUserDetailsToAnonUserEvents',
    'UserAttributes': 'writeUserAttributes',
    'AnonUserAttributes': 'writeAnonUserAttributes',
    'EngagementDetails': 'writeEngagementEventsToUserEvents',
    'DisableEngagementDetails': 'writeDisableEngagementEventsToDisabledUserEvents',
    'UserEvents': 'writeUserDetailsToUserEvents',
    'DisableUserEvents': 'writeDisableUserDetailsToDisableUserEvents',
    'AnonEngagementDetails': 'writeAnonEngagementEventsToAnonUserEvents'
}


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


def get_mongo_connection(host, port, username=None, password=None, auth_source="admin"):
    try:
        uri = f"mongodb://{username}:{password}@{host}:{port}/" if username and password else f"mongodb://{host}:{port}/"
        
        client = MongoClient(uri, serverSelectionTimeoutMS=5000, authSource=auth_source)
        client.admin.command('ping')
        # log_message('INFO', {"msg": "Connected to Mongo"})
        return client
    except ConnectionFailure as e:
        log_message('ERROR', {"msg": "Failed to connect to Mongo", "err": e})
        exit()
    except Exception as e:
        log_message('ERROR', {"msg": "Unexpected error occurred while conneting to mongo", "err": e})
        exit()


def run_shell_command(command):
    try:
        result = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return result.stdout.decode("utf-8")
    except subprocess.CalledProcessError as e:
        raise Exception(f"Shell command error: {e}")
    except subprocess.SubprocessError as e:
        raise Exception(f"Shell subprocess error: {e}")


def parse_count_command_res(command):
    try:
        clients = run_shell_command(command)
        clients = [line.strip().split(",") for line in clients.strip().split("\n")]
    except Exception as e:
        log_message("ERROR", {"mag": "Error executing shell command", "command": command, "error": str(e)})
        exit()

    clients = [[name, int(num) if num.isdigit() else None] for name, num in clients]
    for name, num in clients:
        if num is None:
            log_message("ERROR", {"mag": "client not found", "client": name})
            clients.remove([name, num])
            continue
    
    return clients


def check_attrs_count(panels_file_path, src_mongo_client, dest_mongo_client, ad=None, at=None):
    src_mongo_count_cmd = """perl -lne 'chomp; $cmd="mongosh -u """ + src_mongo_config['user'] + """ -p """ + src_mongo_config['passwd'] + """ -host """ + src_mongo_config["host"] + """ --port """ + str(src_mongo_config['port']) + """ --authenticationDatabase admin $_ --eval=\\047db.userDetails.count()\\047" if $_; $panel=$_ if $_; $panel=$_ if $_; $out=`$cmd`; $out=~ /(\d+)\D*$/; $uid=$1; print "$panel,$uid"' """ + f"""{panels_file_path}"""
    dest_mongo_count_cmd = """perl -lne 'chomp; $cmd="mongosh -u """ + dest_mongo_config['user'] + """ -p """ + dest_mongo_config['passwd'] + """ -host """ + dest_mongo_config["host"] + """ --port """ + str(dest_mongo_config['port']) + """ --authenticationDatabase admin $_ --eval=\\047db.userAttributes.count()\\047" if $_; $panel=$_ if $_; $out=`$cmd`; $out=~ /(\d+)\D*$/; $uid=$1; print "$panel,$uid"' """ + f"""{panels_file_path}"""

    # perl -lne 'chomp; $cmd="mongosh -u root -p icyiguana30  -host nc-mgin3-cfg2.netcore.in --port 27015 --authenticationDatabase admin $_ --eval=\047db.userDetails.count()\047" if $_; $panel=$_ if $_; $out=`$cmd`; $out=~ /(\d+)\D*$/; $uid=$1; print "$panel,$uid"' panels.txt


    src_clients_count = parse_count_command_res(src_mongo_count_cmd)
    dest_clients_count = parse_count_command_res(dest_mongo_count_cmd)

    src_clients_count_dict = dict(src_clients_count)
    dest_clients_count_dict = dict(dest_clients_count)

    for src_client, src_count in src_clients_count_dict.items():
        if src_client not in dest_clients_count_dict:
            log_message("ERROR", {"msg": "Client not found in destination", "client": src_client})
        elif src_count != dest_clients_count_dict[src_client]:
            log_message("ERROR", {"msg": "Attrs count miss match", "client": src_client, "src_count": src_count, "dest_count": dest_clients_count_dict[src_client]})
        else:
            log_message("INFO", {"msg": "Attrs count matched", "client": src_client, "src_count": src_count, "dest_count": dest_clients_count_dict[src_client]})


def check_events_count(panels_file_path, event_counts_file_path, panel_produced_map):
    with open(panels_file_path, "r") as f:
        panels = [line.strip() for line in f if line.strip()]

    with open(event_counts_file_path, "r") as f:
        event_counts = [line.strip() for line in f if line.strip()]
    
    for event_count in event_counts:
        panel_from_file = event_count.split(",")[0]
        method_from_file = event_count.split(",")[1]
        method_name = method_map[method_from_file]
        event_count_value = int(event_count.split(",")[2])

        if panel_from_file not in panels:
            log_message("Warning", {"msg": f"Panel not found in panels file while checking events count", "panel": panel_from_file})
            continue
        
        try:
            if panel_produced_map[panel_from_file+method_name] >= (event_count_value-EVENTS_PADDING*event_count_value) and panel_produced_map[panel_from_file+method_name] <= (event_count_value+EVENTS_PADDING*event_count_value):
                log_message("Info", {"msg": f"{panel_from_file}: EQUAL", "method": method_name, "produced": panel_produced_map[panel_from_file+method_name], "event_count": event_count_value})
            else:
                log_message("Error", {"msg": f"{panel_from_file}: NOT EQUAL", "method": method_name, "produced": panel_produced_map[panel_from_file+method_name], "event_count": event_count_value})
        except KeyError as e:
            log_message("Error", {"msg": f"KeyError: {e}. Panel not found in panel_produced_map."})

def check_produced_eq_consumed(panels_file_path, log_file_path, is_log_enabled=True):
    panel_produced_map = {}
    with open(log_file_path, "r") as f:
        lines = [line.strip() for line in f if line.strip()]
    
    with open(panels_file_path, "r") as f:
        panels = [line.strip() for line in f if line.strip()]

    for i in range(0, len(lines), 2):
        try:
            group_line = lines[i]
            stats_line = lines[i + 1]

            if "Consumer group" in group_line:
                consumer_group = group_line.split("'")[1]
            else:
                log_message("Error", {"msg": f"Skipping malformed group line: {group_line}"})
                continue
        
            panel_from_log = consumer_group.split("_"+str(NUMBER_OF_PARTITIONS)+"_")[0]
            method_from_log = consumer_group.split("_"+str(NUMBER_OF_PARTITIONS)+"_")[1]
            method_from_log = method_from_log.removesuffix('_grp')

            method_name = method_map[method_from_log]
            
            if panel_from_log not in panels:
                log_message("Warning", {"msg": f"Panel not found in panels file while checking produced = consumed", "panel": panel_from_log})
                continue

            parts = stats_line.split()
            consumed = int(parts[3])
            produced = int(parts[5])

            if consumed == produced:
                panel_produced_map[panel_from_log+method_name] = produced
                if is_log_enabled:
                    log_message("Info", {"msg": f"{consumer_group}: EQUAL", "produced": produced, "consumed": consumed})
            else:
                panel_produced_map[panel_from_log+method_name] = -1
                log_message("Error", {"msg": f"{consumer_group}: NOT EQUAL", "produced": produced, "consumed": consumed})

        except Exception as e:
            log_message("ERROR", {"msg": f"File does not follow the expected format. Error parsing lines {i}-{i+1}: {e}"})
    
    return panel_produced_map


def check_produced_eq_consumed_from_log(panels_file_path, consumer_logs_dir, panel_produced_map):
    file_names = []

    with open(panels_file_path, "r") as f:
        panels = [line.strip() for line in f if line.strip()]

    for file_name in os.listdir(consumer_logs_dir):
        if "debug" in file_name and "write" in file_name:
            file_names.append(file_name)
    
    for file_name in file_names:
        panel_from_file_name= file_name.split("-")[1]
        method_from_file_name= file_name.split("-")[2]
        method_from_file_name = method_from_file_name.removesuffix('.log')

        if panel_from_file_name not in panels:
            log_message("Warning", {"msg": f"Panel not found in panels file while checking produced = consumed from consumer log file", "panel": panel_from_file_name})
            continue

        get_count_command = """perl -lne 'while (/\[count = (\d+)\]/g) { $sum += $1 } END { print $sum }' """ + consumer_logs_dir + "/" + file_name 
        try:
            count = int(run_shell_command(get_count_command))
        except ValueError as e:
                count = 0
        except Exception as e:
            log_message("Error", {"msg": f"Unexpected error while executing shell command", "command": get_count_command, "error": str(e)})
            continue
        
        try:
            if panel_produced_map[panel_from_file_name+method_from_file_name] == count:
                log_message("Info", {"msg": f"{file_name}: EQUAL", "produced": panel_produced_map[panel_from_file_name+method_from_file_name], "consumed": count})
            else:
                log_message("Error", {"msg": f"{file_name}: NOT EQUAL", "produced": panel_produced_map[panel_from_file_name+method_from_file_name], "consumed": count})
        except KeyError as e:
            log_message("Error", {"msg": f"KeyError: {e}. Panel not found in panel_produced_map."})


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Migration verification utility.')

    parser.add_argument('--mode', type=str,
                        choices=['attrs_count', 'events_count', 'produced_consumed', 'consumed_count_from_log'],
                        help='Choose which check to perform.')

    parser.add_argument('--log_file', required=True, type=str, help='Log file path for logger setup.')
    parser.add_argument('--panels_file_path', required=True, type=str, help='Path to panels file (for attrs_count).')
    parser.add_argument('--migration_logs_dir', type=str, help='Directory for migration logs (for events_count).')
    parser.add_argument('--pc_count_log_file', type=str, help='Produced-consumed log file (for produced_consumed).')
    parser.add_argument('--consumer_logs_dir', type=str, help='Produced-consumed log file (for produced_consumed).')
    parser.add_argument('--events_count_file', type=str, help='Produced-consumed log file (for produced_consumed).')

    args = parser.parse_args()
    if not args.mode:
        args.mode = None

    if args.log_file:
        setup_logger(args.log_file)

    if args.mode == 'attrs_count':
        if not args.panels_file_path:
            print("Error: --panels_file_path is required for 'attrs_count' mode.")
            # p verify_migration.py --mode=attrs_count --log_file=test.log --panels_file_path=200_panels_part1
            sys.exit(1)

        src_mongo_client = get_mongo_connection( src_mongo_config['host'], src_mongo_config['port'], src_mongo_config['user'], src_mongo_config['passwd'])
        dest_mongo_client = get_mongo_connection(dest_mongo_config['host'], dest_mongo_config['port'],dest_mongo_config['user'], dest_mongo_config['passwd'])

        check_attrs_count(args.panels_file_path, src_mongo_client, dest_mongo_client)

    elif args.mode == 'events_count':
        if not args.events_count_file or not args.pc_count_log_file:
            print("Error: --events_count_file, --pc_count_log_file is required for 'events_count' mode.")
            # p verify_migration.py --mode=events_count --log_file=test.log --pc_count_log_file=test_input.log --panels_file_path=200_panels_part1 --events_count_file=events_count.csv
            sys.exit(1)
        
        panel_produced_map = check_produced_eq_consumed(args.panels_file_path, args.pc_count_log_file, is_log_enabled=False)
        check_events_count(args.panels_file_path, args.events_count_file, panel_produced_map)

    elif args.mode == 'produced_consumed':
        if not args.pc_count_log_file:
            print("Error: --pc_count_log_file is required for 'produced_consumed' mode.")
            # p verify_migration.py --mode=produced_consumed --log_file=test.log --pc_count_log_file=test_input.log
            sys.exit(1)
        check_produced_eq_consumed(args.panels_file_path, args.pc_count_log_file)

    elif args.mode == 'consumed_count_from_log':
        if not args.consumer_logs_dir or not args.pc_count_log_file:
            print("Error: --consumer_logs_dir, --pc_count_log_file is required for 'consumed_count_from_log' mode.")
            # p verify_migration.py --mode=consumed_count_from_log --log_file=test.log --panels_file_path=200_panels_part1
            sys.exit(1)
        
        panel_produced_map = check_produced_eq_consumed(args.panels_file_path, args.pc_count_log_file, is_log_enabled=False)
        check_produced_eq_consumed_from_log(args.panels_file_path, args.consumer_logs_dir, panel_produced_map)
    
    else:
        if not args.consumer_logs_dir or not args.pc_count_log_file or not args.events_count_file:
            print("Error: --consumer_logs_dir, --pc_count_log_file, --events_count_file is required for migration verification")
            print("Example Usage: python3 verify_migration.py --events_count_file=events_count.csv --log_file=test.log --panels_file_path=200_panels_part1 --panels_file_path=200_panels_part1 --consumer_logs_dir=/var/log/apps/mongodataremodel/200_panels_initial_migration_logs/ --pc_count_log_file=test_input.log")
            # p verify_migration.py --log_file=test.log --panels_file_path=200_panels_part1 --panels_file_path=200_panels_part1 --consumer_logs_dir=/var/log/apps/mongodataremodel/200_panels_initial_migration_logs/ --pc_count_log_file=test_input.log
            sys.exit(1)
        
        src_mongo_client = get_mongo_connection( src_mongo_config['host'], src_mongo_config['port'], src_mongo_config['user'], src_mongo_config['passwd'])
        dest_mongo_client = get_mongo_connection(dest_mongo_config['host'], dest_mongo_config['port'],dest_mongo_config['user'], dest_mongo_config['passwd'])
        
        check_attrs_count(args.panels_file_path, src_mongo_client, dest_mongo_client)
        panel_produced_map = check_produced_eq_consumed(args.panels_file_path, args.pc_count_log_file)
        check_produced_eq_consumed_from_log(args.panels_file_path, args.consumer_logs_dir, panel_produced_map)
        check_events_count(args.panels_file_path, args.events_count_file, panel_produced_map)
