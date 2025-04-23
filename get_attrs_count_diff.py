import sys
import re
import subprocess
from tabulate import tabulate

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

def execute_perl_command(host, port, username, password, auth_db, collection_name, panels_file_path):
    perl_command = f"perl -lne 'chomp; $cmd=\"mongosh -u {username} -p \'{password}\' -host {host} --port {port} --authenticationDatabase {auth_db} $_ --eval=\\047db.{collection_name}.count()\\047\" if $_; $panel=$_ if $_; $out=`$cmd`; $out=~ /(\\d+)\\D*$/; print \"$panel,$1\"' {panels_file_path}"
    process = subprocess.Popen(perl_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    stdout, stderr = process.communicate()
    if stderr:
        print(f"Perl error: {stderr}")
        return None
    return stdout.strip().split('\n')

def parse_perl_output(output_lines):
    parsed_data = {}
    for line in output_lines:
        parts = line.split(',')
        if len(parts) == 2:
            parsed_data[parts[0]] = int(parts[1])
    return parsed_data

def process_panels(panels_file_path, src_collection, dest_collection):
    results = []
    zero_diff_count = 0
    five_percent_diff_count = 0
    ten_percent_diff_count = 0
    total_rows = 0

    source_mongo_config = parse_mongo_uri(config_dict['src_mongo_uri'])
    destination_mongo_config = parse_mongo_uri(config_dict['dst_mongo_uri'])

    src_output_lines = execute_perl_command(source_mongo_config['host'], source_mongo_config['port'], source_mongo_config['user'], source_mongo_config['passwd'], auth_db, src_collection, panels_file_path)
    dst_output_lines = execute_perl_command(destination_mongo_config['host'], destination_mongo_config['port'], destination_mongo_config['user'], destination_mongo_config['passwd'], auth_db, dest_collection, panels_file_path)

    if src_output_lines and dst_output_lines:
        src_counts = parse_perl_output(src_output_lines)
        dst_counts = parse_perl_output(dst_output_lines)

        with open(panels_file_path, 'r') as f:
            for line in f:
                total_rows += 1
                panel = line.strip()
                src_count = src_counts.get(panel)
                dst_count = dst_counts.get(panel)

                if src_count is not None and dst_count is not None:
                    diff = src_count - dst_count
                    if diff == 0 and src_count != 0:
                        zero_diff_count += 1
                        continue
                    deviation_percentage = 0
                    if src_count != 0:
                        deviation_percentage = abs(diff) / src_count * 100
                    row = [panel, src_count, dst_count, diff]
                    if deviation_percentage > 10:
                        row.append("red")
                        ten_percent_diff_count += 1
                    elif deviation_percentage > 5:
                        row.append("orange")
                        five_percent_diff_count += 1
                    else:
                        row.append(None)
                    results.append(row)

    return results, zero_diff_count, five_percent_diff_count, ten_percent_diff_count, total_rows

def format_table(results):
    headers = ["panel_name", "mongo5 count", "mongo7 count", "diff (mongo5-mongo7)"]
    colored_rows = []
    for row in results:
        colored_row = list(row[:4])
        if len(row) > 4 and row[4] == "red":
            colored_row = [f"\033[91m{item}\033[0m" for item in colored_row]
        elif len(row) > 4 and row[4] == "orange":
            colored_row = [f"\033[93m{item}\033[0m" for item in colored_row]
        colored_rows.append(colored_row)
    return tabulate(colored_rows, headers=headers, tablefmt="pipe")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script_name.py <property_file> <panels_file_path>")
        sys.exit(1)

    property_file = sys.argv[1]
    panels_file_path = sys.argv[2]
    auth_db = "admin"
    source_collection = "userDetails"
    dest_collection = "userAttributes"

    results, zero_diff_count, five_percent_diff_count, ten_percent_diff_count, total_rows = process_panels(
        panels_file_path, source_collection, dest_collection
    )
    table = format_table(results)
    print(table)
    print(f"\nPanels count: {total_rows}")
    print(f"Panels with 0 difference (and non-zero counts): {zero_diff_count}")
    print(f"Panels with >5% difference: {five_percent_diff_count}")
    print(f"Panels with >10% difference: {ten_percent_diff_count}")
