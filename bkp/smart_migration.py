import subprocess
import pandas as pd
from dotenv import load_dotenv
from langgraph.prebuilt import create_react_agent

load_dotenv()

def execute_shell_command(command):
    """
    Execute a shell command and return the output.
    """
    return subprocess.check_output(command, shell=True).decode('utf-8')


import subprocess
import logging
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def service_exists(service_name):
    """Check if a systemd service exists."""
    try:
        output = execute_shell_command(f'sudo systemctl list-unit-files {service_name}.service')
        return f"{service_name}.service" in output
    except subprocess.CalledProcessError:
        return False

def stop_kafka_service():
    """Stop the Kafka service if it exists."""
    if service_exists('kafka'):
        try:
            execute_shell_command('sudo systemctl stop kafkwhen it finds a match, and the systemctl list-unit-files command needs to be run witha')
        except subprocess.CalledProcessError as e:
            logger.warning(f"Failed to stop Kafka: {str(e)}")
    else:
        logger.info("Kafka service does not exist, skipping stop")

def stop_zookeeper_service():
    """Stop the Zookeeper service if it exists."""
    if service_exists('zookeeper'):
        try:
            execute_shell_command('sudo systemctl stop zookeeper')
        except subprocess.CalledProcessError as e:
            logger.warning(f"Failed to stop Zookeeper: {str(e)}")
    else:
        logger.info("Zookeeper service does not exist, skipping stop")

def remove_directory(directory):
    """Remove a directory if it exists."""
    try:
        execute_shell_command(f'sudo rm -rf {directory}')
    except subprocess.CalledProcessError as e:
        logger.warning(f"Failed to remove {directory}: {str(e)}")

def create_directory(directory):
    """Create a directory with full permissions."""
    try:
        execute_shell_command(f'sudo mkdir -p {directory}')
        execute_shell_command(f'sudo chmod -R 777 {directory}')
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to create directory {directory}: {str(e)}")

def start_kafka_service():
    """Start the Kafka service and verify it's running."""
    if service_exists('kafka'):
        try:
            execute_shell_command('sudo systemctl start kafka')
            status = execute_shell_command('sudo systemctl is-active kafka')
            if 'active' not in status.lower():
                raise RuntimeError("Kafka service is not active")
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Failed to start Kafka: {str(e)}")
    else:
        logger.info("Kafka service does not exist, skipping start")

def start_zookeeper_service():
    """Start the Zookeeper service and verify it's running."""
    if service_exists('zookeeper'):
        try:
            execute_shell_command('sudo systemctl start zookeeper')
            status = execute_shell_command('sudo systemctl is-active zookeeper')
            if 'active' not in status.lower():
                raise RuntimeError("Zookeeper service is not active")
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Failed to start Zookeeper: {str(e)}")
    else:
        logger.info("Zookeeper service does not exist, skipping start")

def verify_directory_exists(directory):
    """Verify that a directory exists."""
    if not os.path.exists(directory):
        raise RuntimeError(f"Directory {directory} does not exist")

def automate_kafka_zookeeper_reset():
    """Reset Kafka and Zookeeper services and their data directories.
    
    This function performs a complete reset of Kafka and Zookeeper by:
    1. Stopping both services
    2. Removing existing data directories
    3. Creating new directories with proper permissions
    4. Starting both services
    5. Verifying the services are running and directories exist
    """
    try:
        logger.info("Stopping services...")
        stop_kafka_service()
        stop_zookeeper_service()

        data_dir = '/data'
        kafka_logs_dir = os.path.join(data_dir, 'kafka-logs')
        zookeeper_dir = os.path.join(data_dir, 'zookeeper')

        logger.info("Removing data directories...")
        remove_directory(kafka_logs_dir)
        remove_directory(zookeeper_dir)

        logger.info("Creating new directories...")
        create_directory(data_dir)
        create_directory(kafka_logs_dir)
        create_directory(zookeeper_dir)

        logger.info("Starting services...")
        start_zookeeper_service()
        start_kafka_service()

        logger.info("Verifying directories...")
        verify_directory_exists(kafka_logs_dir)
        verify_directory_exists(zookeeper_dir)

        logger.info("Reset completed successfully")

    except Exception as e:
        logger.error(f"Error during reset: {str(e)}")
        raise


def start_redis_server():
    """
    Start the Redis server.
    """
    execute_shell_command('sudo systemctl start redis-server')


def stop_redis_server():
    """
    Stop the Redis server.
    """
    execute_shell_command('sudo systemctl stop redis-server')


automate_kafka_zookeeper_reset()


