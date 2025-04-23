import redis
import json
import pandas as pd
from tabulate import tabulate
import sys
import ast
from datetime import datetime
import numpy as np

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

def fetch_and_display_redis_data(file_name=None, selected_columns=None, **filters):
    r = redis.Redis(host=redis_config['redis_host'], port=redis_config['redis_port'], db=redis_config['redis_db'], decode_responses=True)

    # all_keys = r.keys("producer_*") + r.keys("consumer_*")
    if file_name is not None:
        with open(file_name, 'r') as f:
            panels = [panel.strip() for panel in f]
                    
            all_keys = set()
            for panel in panels:
                consumer_redis_key = "consumer_" + panel
                all_keys.add(consumer_redis_key)
    # all_keys = r.keys("consumer_*")
    else:
        all_keys = r.keys("consumer_*")

    records = []

    for key in all_keys:
        data = r.hgetall(key)

        for field, value in data.items():
            try:
                parsed_value = json.loads(value)

                if isinstance(parsed_value, dict):
                    parsed_value = [parsed_value]

                if isinstance(parsed_value, list):
                    for entry in parsed_value:
                        if isinstance(entry, dict):
                            entry = entry.copy()
                            entry["redis_key"] = key
                            entry["event_type"] = field
                            records.append(entry)
                else:
                    print(f"Skipping field '{field}' in {key}, unexpected type: {type(parsed_value)}")

            except json.JSONDecodeError:
                print(f"Skipping field '{field}' in {key}, not a valid JSON")

    df = pd.DataFrame(records)

    if df.empty:
        print("No data found.")
        return
    
    
    df["start_time"] = pd.to_datetime(df["start_time"], format="%Y-%m-%d %H:%M:%S.%f")
    df["update_time"] = pd.to_datetime(df["update_time"], format="%Y-%m-%d %H:%M:%S.%f")
    
    df["time_diff_mins"] = (( df["update_time"] - df["start_time"]).dt.total_seconds() / 60).round(3)
    
    df["consumed_per_hour"] = np.where(
        df["current_consumer_offset"] != -1, 
        (df["current_consumer_offset"] / df["time_diff_mins"]) * 60, 
        0
    )

    df["produced_per_hour"] = np.where(
        df["current_producer_offset"] != -1, 
        (df["current_producer_offset"] / df["time_diff_mins"]) * 60, 
        0
    )
    
    df["consumed_per_hour"] = df["consumed_per_hour"].astype(int)
    df["consumed_per_hour"] = df["consumed_per_hour"].abs()
    
    df["produced_per_hour"] = df["produced_per_hour"].astype(int)
    df["produced_per_hour"] = df["produced_per_hour"].abs()
    
    selected_columns.append("time_diff_mins")
    selected_columns.append("consumed_per_hour")
    selected_columns.append("produced_per_hour")

    for key, value in filters.items():
        if key in df.columns:
            df = df[df[key] == value]

    if selected_columns:
        df = df[selected_columns]
    
    print(tabulate(df, headers='keys', tablefmt='pretty', showindex=False))


if __name__ == '__main__':
    filters_dict = {'status': 'running'}
    if len(sys.argv) == 2:
        panel_file_path = sys.argv[1]
        selected_columns=["update_time", "status", "group_name", "current_producer_offset", "current_consumer_offset", "prev_lag", "current_lag"]
    elif len(sys.argv) == 3:
        panel_file_path = sys.argv[1]
        selected_columns = ast.literal_eval(sys.argv[2])
        # print(selected_columns, type(selected_columns))
        if not isinstance(selected_columns, list):
            print("Error: Argument is not a valid list")
            exit()
    else:
        panel_file_path = sys.argv[1]
        selected_columns = ast.literal_eval(sys.argv[2])
        if not isinstance(selected_columns, list):
            print("Error: Argument is not a valid list")
            exit()

        for arg in sys.argv[3:]:  # Start from the second argument
            if "=" in arg:
                key, value = arg.split("=", 1)  # Split only on the first '='
                filters_dict[key] = value
    
    
    fetch_and_display_redis_data(
        panel_file_path,
        selected_columns=selected_columns,
        **filters_dict
    )
