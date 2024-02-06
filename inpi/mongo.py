import os
from pymongo import MongoClient
from retry import retry
from timeit import default_timer as timer

from utils.utils import to_jsonl

from application.server.main.logger import get_logger

logger = get_logger(__name__)

DATA_PATH = os.getenv("MOUNTED_VOLUME_TEST")
INPI_PATH = os.path.join(DATA_PATH, "INPI")
INPI_DB = "inpi"
INPI_INDEX = "publication-number"


def mongo_delete_collections(collections_names=[]):
    """Delete collections from mongo db.

    Args:
        collections_names (list, optional): _description_. Defaults to [].
    """
    delete_timer = timer()

    mongo_client = MongoClient(os.getenv("MONGO_URI"))
    mongo_inpi = mongo_client[INPI_DB]

    if not collections_names:
        collections_names = mongo_inpi.list_collection_names()

    logger.debug(f"Start delete mongo collections {collections_names}")

    for collection_name in collections_names:
        mongo_inpi.drop_collection(collection_name)
    mongo_client.close()

    logger.debug(f"Mongo collections dropped in {timer() - delete_timer}")


def mongo_delete(collection_data, collection_name):
    """Delete data from a mongo collection.

    Args:
        collection_data (_type_): _description_
        collection_name (_type_): _description_
    """
    delete_timer = timer()

    mongo_client = MongoClient(os.getenv("MONGO_URI"))
    mongo_inpi = mongo_client[INPI_DB]
    collection = mongo_inpi[collection_name]

    logger.debug(f"Collection {collection_name} start delete of {len(collection_data)} records")

    identifiers = list(set([record.get(INPI_INDEX) for record in collection_data]))
    logger.debug(f"Collection {collection_name} start delete of {len(identifiers)} identifiers: {identifiers[:3]}...")

    collection.delete_many({INPI_INDEX: {"$in": identifiers}})
    mongo_client.close()

    logger.info(f"Collection {collection_name} deleted in {(timer() - delete_timer):.2f}")


def mongo_create_index(collection_name, index_name):
    """Create index on a mongo collection.

    Args:
        collection_name (_type_): _description_
        index_name (_type_): _description_
    """
    create_index_timer = timer()
    mongo_client = MongoClient(os.getenv("MONGO_URI"))
    mongo_inpi = mongo_client[INPI_DB]
    collection = mongo_inpi[collection_name]

    logger.info(f"Collection {collection_name} start indexing by '{index_name}'")
    collection.create_index(index_name, unique=True)
    mongo_client.close()

    logger.info(f"Collection {collection_name} indexed in {(timer() - create_index_timer):.2f}")


@retry(delay=200, tries=3)
def mongo_import_collection(collection_name, collection_data):
    """Import collection data into mongo db.

    Args:
        collection_name (_type_): _description_
        collection_data (_type_): _description_
    """
    if not collection_data:
        logger.warn(f"Collection {collection_name} not imported. Collection data is empty")
        return

    logger.debug(f"Collection {collection_name} contains {len(collection_data)} records to add")
    # logger.debug(f"{collection}")

    # Delete from mongo
    mongo_delete(collection_data, collection_name)

    # Save data as json
    output_json = os.path.join(INPI_PATH, f"{collection_name}.jsonl")
    logger.debug(f"Collection {collection_name} output json : {output_json}")
    to_jsonl(collection_data, output_json, "w")

    # Import to mongo
    import_timer = timer()
    mongoimport = f'mongoimport --numInsertionWorkers 2 --uri "{os.getenv("MONGO_URI")}" \
        --collection {collection_name} --file {output_json}'
    os.system(mongoimport)
    logger.debug(f"Collection {collection_name} loaded in {(timer() - import_timer):.2f}")

    # Remove json
    os.remove(output_json)


def mongo_import(data):
    """Import data into mongo db.

    Args:
        data (_type_): _description_
    """

    if not data:
        logger.warn("Import data is empty!")
        return

    COLLECTIONS_NAMES = data[0].keys()
    collections_timer = timer()
    logger.info(f"Start mongo import of {COLLECTIONS_NAMES}")

    # Get collection records from data
    for collection_name in COLLECTIONS_NAMES:
        logger.debug(f"Collection {collection_name} start loading")
        collection_data = []
        for record in data:
            if collection_name in record:
                collection_data += record[collection_name]

        # Import collection
        mongo_import_collection(collection_name, collection_data)

    logger.info(f"Collections imported in {(timer() - collections_timer):.2f}")
