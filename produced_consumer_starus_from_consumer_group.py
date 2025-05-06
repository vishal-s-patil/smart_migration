import subprocess
import sys
import time
import re
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


KAFKA_BROKER = config_dict['kafka_bootstrap_servers']

def execute_kafka_consumer_command(bootstrap_servers, group_name, kafka_home):
    """Executes the custom kafka-consumer-groups.sh command and returns output/error."""
    command = [
        f"{kafka_home}/bin/kafka-consumer-groups.sh",
        "--describe",
        "--group",
        group_name,
        "--bootstrap-server",
        bootstrap_servers
    ]
    awk_command = [
        "awk",
        "-F",
        " ",
        '($4 != "-" && $4 >= 0 && $5 != "-" && $5 >= 0 && $6 != "-" && $6 >= 0){count1 += $4;count2 +=$5;count3 +=$6;d=strftime("%Y-%m-%d %H:%M:%S");} END {if (d) print d,"Consumed:",count1, "Produced:",count2,"Lag:",count3; else print "Consumed:",count1, "Produced:",count2,"Lag:",count3;}'
    ]

    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    awk_process = subprocess.Popen(awk_command, stdin=process.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    process.stdout.close()

    stdout, stderr = awk_process.communicate()
    return stdout.strip(), stderr.strip(), awk_process.returncode

def format_log_entry(group_name, output):
    """Formats the log entry based on the output format and content."""
    if re.match(r"^Consumed:\s+\d+\s+Produced:\s+\d+\s+Lag:\s+\d+$", output):
        return f"[WARNING] {group_name} :: {output}"
    elif re.match(r"^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\s+Consumed:\s+(\d+)\s+Produced:\s+(\d+)\s+Lag:\s+(\d+)$", output):
        match = re.match(r"^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\s+Consumed:\s+(\d+)\s+Produced:\s+(\d+)\s+Lag:\s+(\d+)$", output)
        consumed = int(match.group(1))
        produced = int(match.group(2))
        lag = int(match.group(3))
        if produced == consumed and lag == 0:
            return f"[INFO] {group_name} :: {output}"
        else:
            return f"[WARNING] {group_name} :: {output}"
    else:
        return f"[INFO] {group_name} :: {output}" # Default to INFO for other outputs

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script_name.py <panel_file> <output_file>")
        sys.exit(1)

    panel_file = sys.argv[1]
    output_file = sys.argv[2]
    bootstrap_servers = KAFKA_BROKER
    num_partitions = "10"
    kafka_home = "/usr/local/kafka_2.13-2.6.2"

    try:
        with open(panel_file, 'r') as f_panel, open(output_file, 'w') as f_out:
            panels = [line.strip() for line in f_panel]
            total_error_count = 0

            for panel in panels:
                print(f"Processing panel: {panel}")
                f_out.write(f"Processing panel: {panel}\n")

                group_names = [
                    f"{panel}_{num_partitions}_AnonEngagementDetails_grp",
                    f"{panel}_{num_partitions}_AnonUserAttributes_grp",
                    f"{panel}_{num_partitions}_AnonUserEvents_grp",
                    f"{panel}_{num_partitions}_DisableEngagementDetails_grp",
                    f"{panel}_{num_partitions}_DisableUserAttributes_grp",
                    f"{panel}_{num_partitions}_DisableUserEvents_grp",
                    f"{panel}_{num_partitions}_EngagementDetails_grp",
                    f"{panel}_{num_partitions}_UserAttributes_grp",
                    f"{panel}_{num_partitions}_UserEvents_grp",
                ]

                for group_name in group_names:
                    print(f"  Describing group: {group_name}")
                    f_out.write(f"  Describing group: {group_name}\n")

                    stdout, stderr, returncode = execute_kafka_consumer_command(bootstrap_servers, group_name, kafka_home)

                    if returncode == 0:
                        if stdout:
                            log_entry = format_log_entry(group_name, stdout)
                            f_out.write(f"    {log_entry}\n")
                            print(f"    {log_entry}")
                        elif stderr:
                            f_out.write(f"    Info/Warning: {stderr}\n")
                            print(f"    Info/Warning: {stderr}")
                    else:
                        total_error_count += 1
                        f_out.write(f"    Error (Return Code: {returncode}): {stderr}\n")
                        print(f"    Error (Return Code: {returncode}): {stderr}")

                    time.sleep(1)

            f_out.write(f"\nTotal Errors Encountered: {total_error_count}\n")
            print(f"\nTotal Errors Encountered: {total_error_count}")
            print(f"\nOutput and errors written to: {output_file}")

    except FileNotFoundError:
        print(f"Error: Panel file '{panel_file}' not found.")
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        sys.exit(1)