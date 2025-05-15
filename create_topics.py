from kafka.admin import KafkaAdminClient, NewTopic
import argparse
import sys

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

def create_kafka_topic(topic_name, bootstrap_servers, num_partitions=10, replication_factor=1):
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

    topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)

    try:
        admin_client.create_topics([topic])
        print(f"Topic '{topic_name}' created successfully with {num_partitions} partitions.")
    except Exception as e:
        print(f"Error creating topic: {e}")
    finally:
        admin_client.close()

if __name__ == "__main__":
    # if len(sys.argv) == 3:
    #     panels_file_name = sys.argv[1]
    #     number_of_partitions = int(sys.argv[2])
    #     replication_factor = 1

    parser = argparse.ArgumentParser(description="Process input files and parameters.")
    parser.add_argument("panels_file_name", help="Name of the panels file")
    parser.add_argument("number_of_partitions", type=int, help="Number of partitions")
    parser.add_argument("replication_factor", type=int, help="Replication factor", default=10)
    parser.add_argument("--methods", help="Comma-separated list of methods", default=None)

    args = parser.parse_args()

    panels_file_name = args.panels_file_name
    number_of_partitions = args.number_of_partitions
    replication_factor = args.replication_factor
    methods = args.methods.split(",") if args.methods else ['AnonEngagementDetails', 'AnonUserAttributes', 'AnonUserEvents', 'DisableEngagementDetails', 'DisableUserAttributes', 'DisableUserEvents', 'EngagementDetails', 'UserAttributes', 'UserEvents']

    
    with open(panels_file_name, 'r') as f:
        panels = [panel.strip() for panel in f]
        for panel in panels:
            for method in methods:
                topic = panel + '_' + str(number_of_partitions) + '_' + method
                create_kafka_topic(topic, config_dict['kafka_bootstrap_servers'], number_of_partitions)
