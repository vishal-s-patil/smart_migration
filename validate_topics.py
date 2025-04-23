from kafka.admin import KafkaAdminClient
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

def get_topic_details(admin_client, topic_name):
    try:
        topic_metadata = admin_client.describe_topics([topic_name])[0]
        num_partitions = len(topic_metadata['partitions'])
        replication_factor = len(topic_metadata['partitions'][0]['replicas'])
        
        return num_partitions, replication_factor

    except Exception as e:
        print(f"Error fetching topic details: {e}")
        return None, None


if __name__ == "__main__":
    if len(sys.argv) == 3:
        panels_file_name = sys.argv[1]
        number_of_partitions = int(sys.argv[2])
        replication_factor = 1

    methods = ['AnonEngagementDetails', 'AnonUserAttributes', 'AnonUserEvents', 'DisableEngagementDetails', 'DisableUserAttributes', 'DisableUserEvents', 'EngagementDetails', 'UserAttributes', 'UserEvents']
    admin_client = KafkaAdminClient(bootstrap_servers=config_dict['kafka_bootstrap_servers'])

    with open(panels_file_name, 'r') as f:
        panels = [panel.strip() for panel in f]
        for panel in panels:
            for method in methods:
                topic = panel + '_' + str(number_of_partitions) + '_' + method
                current_num_partitions, current_replication_factor = get_topic_details(admin_client, topic)
                if current_num_partitions is not None and current_replication_factor is not None:
                    if replication_factor != current_replication_factor:
                        print(f"Error: Replication factor mismatch for topic: {topic}")
                    else:
                        print(f'Info: replication factor matches for topic: {topic}')
                    if number_of_partitions != current_num_partitions:
                        print(f"Error: number of partitions mismatch for topic: {topic}")
                    else:
                        print(f'Info: number of partitions matches for topic: {topic}')  
                else:
                    print(f"Error fetching topic details for topic: {topic}")

    admin_client.close()
