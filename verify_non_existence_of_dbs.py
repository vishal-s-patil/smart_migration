import pymongo
import sys
import logging

def verify_non_existent_databases(mongo_uri, panels_file, log_file):
    """
    Verifies that the databases listed in the panels file do not exist
    on the MongoDB server. Logs the status to the specified log file.

    Args:
        mongo_uri (str): The MongoDB connection URI.
        panels_file (str): Path to the file containing a list of database names (one per line).
        log_file (str): Path to the log file to write output.
    """
    logging.basicConfig(filename=log_file, level=logging.INFO,
                        format='[%(levelname)s] [db:%(name)s] [msg:%(message)s]')
    logger = logging.getLogger(__name__)

    try:
        client = pymongo.MongoClient(mongo_uri)
        logger.info(f"Connected to MongoDB: {mongo_uri}", extra={'name': 'connection'})
        existing_databases = client.list_database_names()

        try:
            with open(panels_file, 'r') as f:
                dbs_to_check = [line.strip() for line in f if line.strip()]
        except FileNotFoundError:
            logger.error(f"File '{panels_file}' not found.", extra={'name': 'file_read'})
            return

        if not dbs_to_check:
            logger.info(f"No databases listed in '{panels_file}' to check.", extra={'name': 'file_read'})
            return

        for db_name in dbs_to_check:
            if db_name in existing_databases:
                logger.error(f"Database '{db_name}' exists (should not exist).", extra={'name': db_name})
            else:
                logger.info(f"Database '{db_name}' does not exist (as expected).", extra={'name': db_name})

    except pymongo.errors.ConnectionFailure as e:
        logger.error(f"Could not connect to MongoDB: {e}", extra={'name': 'connection'})
    finally:
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 verify_non_existence_of_dbs.py <panels_file> <log_file_path>")
        sys.exit(1)

    panels_file = sys.argv[1]
    log_file_path = sys.argv[2]

    # Replace with your actual MongoDB URI
    mongo_uri = "your_mongodb_uri"

    verify_non_existent_databases(mongo_uri, panels_file, log_file_path)
    