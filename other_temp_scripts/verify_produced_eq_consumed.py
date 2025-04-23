import subprocess
import re

KAFKA_BROKER = "localhost:9092"
LOG_FILE = "all_again_produced_eq_consumed.log"

def run_command(command):
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        return result.stdout.strip()
    except Exception as e:
        return str(e)

def parse_log_line(log_line):
    match = re.search(r'Consumed:\s*(\d+)?\s*Produced:\s*(\d+)?', log_line)
    if match:
        consumed = int(match.group(1)) if match.group(1) else 0
        produced = int(match.group(2)) if match.group(2) else 0
        return consumed, produced
    return None, None

def check_produced_eq_conumed(group_name, log_handle):
    command = """/usr/local/kafka_2.13-2.6.2/bin/kafka-consumer-groups.sh --describe --group """ + group_name + """ --bootstrap-server """ + KAFKA_BROKER + """ | awk -F " " '($4 != "-" && $4 >= 0 && $5 != "-" && $5 >= 0 && $6 != "-" && $6 >= 0){count1 += $4;count2 +=$5;count3 +=$6;d=strftime("%Y-%m-%d %H:%M:%S");} END {print d,"Consumed:",count1, "Produced:",count2,"Lag:",count3;}'"""
    output = run_command(command)
    produced, consumed = parse_log_line(output)
    if produced is None or consumed is None:
        log_handle.write(f'Produced is None: group: {group_name}\n')
    elif produced != consumed:
        log_handle.write(f'pro: {produced}, con: {consumed}, group: {group_name}\n')
    else:
        log_handle.write('Info: produced=consumed.')

if __name__ == '__main__':
    with open('all_group_names.txt', 'r') as file:
        group_names = file.read().splitlines()

    with open(LOG_FILE, 'a') as log_file:
        for group_name in group_names:
            check_produced_eq_conumed(group_name, log_file)
