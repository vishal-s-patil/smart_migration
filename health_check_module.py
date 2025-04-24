# health_check_module.py

import configparser
import sys
from typing import List, Dict, Optional
import logging
import redis
from kafka.admin import NewTopic
from pymongo import MongoClient
from kafka import KafkaProducer, KafkaAdminClient
from kafka.errors import NoBrokersAvailable, KafkaTimeoutError, TopicAlreadyExistsError


logger = logging.getLogger(__name__)


def setup_logging(log_file_path: str) -> None:
    """Configure logging with specified file path."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(lineno)d - %(message)s',
        filename=log_file_path,
        filemode='a'
    )

def check_redis_connectivity(redis_host: str, redis_port: int = 6379, redis_db: int = 0) -> bool:
    try:
        r = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
        r.ping()
        logger.info(f"Successfully connected to Redis at {redis_host}:{redis_port}")
        return True
    except redis.exceptions.ConnectionError as e:
        logger.error(f"Redis connection error for {redis_host}:{redis_port}: {e}")
        return False
    except Exception as e:
        logger.error(f"An unexpected error occurred during Redis connection check for {redis_host}:{redis_port}: {e}")
        return False


def check_mongo_connectivity(mongo_uris: List[str], serverSelectionTimeoutMS: int = 5000) -> bool:
    """
    Checks connectivity to all MongoDB servers.
    Returns True only if all servers are accessible.
    """
    if not mongo_uris:
        logger.error("No MongoDB URIs provided for connectivity check")
        return False

    all_connected = True
    for url in mongo_uris:
        client = None
        try:
            client = MongoClient(url, serverSelectionTimeoutMS=serverSelectionTimeoutMS)
            client.admin.command('ping')
            logger.info(f"Successfully connected to MongoDB at: {url}")
        except Exception as e:
            logger.error(f"MongoDB connection error for {url}: {e}")
            all_connected = False
        finally:
            if client:
                try:
                    client.close()
                except Exception as e:
                    logger.warning(f"Error closing MongoDB client for {url}: {e}")

    return all_connected


def check_kafka_connectivity(
    bootstrap_servers: List[str],
    timeout: int = 5,
    test_topic: str = '__health_check_topic',
    num_partitions: int = 1,
    replication_factor: int = 1
) -> bool:
    """
    Checks the connectivity to all Kafka brokers.
    Returns True only if all brokers are accessible.

    Args:
        bootstrap_servers: A list of Kafka broker addresses.
        timeout: Connection and operation timeout in seconds.
        test_topic: The name of the topic to create for the health check.
        num_partitions: The number of partitions for the test topic.
        replication_factor: The replication factor for the test topic.

    Returns:
        bool: True if all brokers are accessible, False otherwise.
    """
    if not bootstrap_servers:
        logger.error("No bootstrap servers provided for Kafka connectivity check")
        return False

    # Validate bootstrap servers format
    valid_servers = []
    for server in bootstrap_servers:
        try:
            host, port = server.split(':')
            if not host or not port:
                logger.warning(f"Invalid server format: {server}")
                continue
            valid_servers.append(server)
        except ValueError:
            logger.warning(f"Invalid server format: {server}")
            continue

    if not valid_servers:
        logger.error("No valid bootstrap servers found")
        return False

    all_brokers_healthy = True
    admin_client = None
    producer = None

    # Check each broker individually
    for server in valid_servers:
        try:
            # Try to create admin client for this specific broker
            admin_client = KafkaAdminClient(
                bootstrap_servers=[server],
                client_id=f'health-check-admin-client-{server}',
                request_timeout_ms=timeout * 1000
            )
            
            # Try to list topics to verify connection
            admin_client.list_topics()
            logger.info(f"Successfully connected to Kafka broker: {server}")
            
        except NoBrokersAvailable as e:
            logger.error(f"Kafka broker {server} is not available: {e}")
            all_brokers_healthy = False
        except KafkaTimeoutError as e:
            logger.error(f"Kafka connection timeout for broker {server}: {e}")
            all_brokers_healthy = False
        except Exception as e:
            logger.error(f"An unexpected error occurred while checking Kafka broker {server}: {e}")
            all_brokers_healthy = False
        finally:
            if admin_client:
                try:
                    admin_client.close()
                except Exception as e:
                    logger.warning(f"Error closing Kafka admin client for {server}: {e}")

    # If all brokers are healthy, try to create topic and test producer
    if all_brokers_healthy:
        try:
            # Create admin client for all brokers
            admin_client = KafkaAdminClient(
                bootstrap_servers=valid_servers,
                client_id='health-check-admin-client',
                request_timeout_ms=timeout * 1000
            )
            
            # Try to create a test topic
            topic_list = [NewTopic(
                name=test_topic,
                num_partitions=num_partitions,
                replication_factor=replication_factor
            )]
            admin_client.create_topics(
                new_topics=topic_list,
                timeout_ms=timeout * 1000,
                validate_only=False
            )
            
            # Test producer with all brokers
            producer = KafkaProducer(
                bootstrap_servers=valid_servers,
                request_timeout_ms=timeout * 1000
            )
            producer.send(test_topic, b'test_message')
            producer.flush(timeout=timeout)
            
            logger.info(f"Successfully verified all Kafka brokers and created test topic: {valid_servers}")
            return True
        except TopicAlreadyExistsError as e:
            logger.info(f"Topic already exists: {e}")
            return True
        except Exception as e:
            logger.error(f"Failed to create topic or test producer with all brokers: {e}")
            return False
        finally:
            # delete the test topic
            admin_client.delete_topics([test_topic])
            if admin_client:
                try:
                    admin_client.close()
                except Exception as e:
                    logger.warning(f"Error closing Kafka admin client: {e}")
            if producer:
                try:
                    producer.close()
                except Exception as e:
                    logger.warning(f"Error closing Kafka producer: {e}")
    else:
        return False


def load_config(property_file: str) -> Dict[str, str]:
    """
    Load configuration from a property file.
    
    Args:
        property_file: Path to the property file
        
    Returns:
        Dictionary containing configuration key-value pairs
    """
    config_data = {}
    try:
        with open(property_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):  # Ignore empty lines and comments
                    if '=' in line:
                        key, value = line.split('=', 1)
                        config_data[key.strip()] = value.strip()
    except FileNotFoundError:
        logger.error(f"Property file not found: {property_file}")
    except Exception as e:
        logger.error(f"Error reading property file {property_file}: {e}")

    return config_data


def health_check(property_file: str, log_file_path: str) -> None:
    """
    Perform health checks for Redis, MongoDB, and Kafka based on configuration.
    All servers must be active for a successful health check.
    """
    setup_logging(log_file_path)
    config = load_config(property_file)

    if not config:
        logger.error("Failed to load configuration.")
        return

    # Check Redis connectivity
    redis_healthy = True
    if 'redis_uri' in config and 'redis_port' in config:
        if not check_redis_connectivity(config['redis_uri'], int(config['redis_port'])):
            logger.error("Redis is down.")
            redis_healthy = False
        else:
            logger.info("Redis is healthy.")
    else:
        logger.error("Redis configuration missing.")
        redis_healthy = False

    # Check MongoDB connectivity
    mongo_healthy = True
    mongo_uris = []
    if 'src_mongo_uri' in config:
        mongo_uris.append(config['src_mongo_uri'])
    if 'dst_mongo_uri' in config:
        mongo_uris.append(config['dst_mongo_uri'])

    if mongo_uris:
        if not check_mongo_connectivity(mongo_uris):
            logger.error("MongoDB is down (one or more instances unreachable).")
            mongo_healthy = False
        else:
            logger.info("MongoDB is healthy (all instances reachable).")
    else:
        logger.error("No MongoDB URIs found in the configuration.")
        mongo_healthy = False

    # Check Kafka connectivity
    kafka_healthy = True
    if 'kafka_bootstrap_servers' in config:
        bootstrap_servers = [server.strip() for server in config['kafka_bootstrap_servers'].split(',')]
        logger.info(f"Checking Kafka connectivity for all brokers: {bootstrap_servers}")
        if not check_kafka_connectivity(bootstrap_servers):
            logger.error("Kafka is down (one or more brokers unreachable).")
            kafka_healthy = False
        else:
            logger.info("Kafka is healthy (all brokers reachable).")
    else:
        logger.error("No Kafka bootstrap servers found in the configuration.")
        kafka_healthy = False

    # Overall health status
    if redis_healthy and mongo_healthy and kafka_healthy:
        logger.info("All services are healthy.")
        return True, "All services are healthy."
    else:
        logger.error("One or more services are unhealthy.")
        return False, f"Redis: {redis_healthy}, MongoDB: {mongo_healthy}, Kafka: {kafka_healthy}"

def main() -> None:
    """Main entry point for the health check module."""
    # Default log file path
    default_log_file = "logs/health_check.log"
    
    if len(sys.argv) < 2:
        print("Usage: python3 health_check_module.py <property_file_path> [log_file_path]")
        print(f"Note: If log_file_path is not provided, will use default: {default_log_file}")
        sys.exit(1)
    
    try:
        property_file = sys.argv[1]
        # Use provided log file path or default
        log_file_path = sys.argv[2] if len(sys.argv) > 2 else default_log_file
        health_check(property_file, log_file_path)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
