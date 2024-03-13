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

COLLECTIONS_CONFIG = {
    "amendedClaim": {},
    "application": {},
    "citation": {},
    "cpc": {},
    "errata": {},
    "inscription": {},
    "ipc": {},
    "oldIpc": {},
    "person": {"with_history": True},
    "priority": {},
    "publication": {"unique": True},
    "publicationRef": {},
    "relatedDocument": {},
    "renewal": {},
    "search": {},
}


def mongo_collections_find_field(field:str, include=True):
    """Find collections where field is true.

    Args:
        field: Config field.
        include (optional): Should found collections be included. Defaults to True.

    Returns:
        List of collections.
    """
    collections_with_field = [
        collection_name
        for collection_name, collection_config in COLLECTIONS_CONFIG.items()
        if collection_config.get(field)
    ]

    if not include:
        collections_without_field = list(set(COLLECTIONS_CONFIG.keys()) - set(collections_with_field))
        return collections_without_field

    return collections_with_field


def mongo_delete_collections(collections_names=None):
    """ Delete collections from mongo db.

    Args:
        collections_names (optional): List of collections to delete. Defaults to all.
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

    logger.debug(f"Mongo collections dropped in {(timer() - delete_timer):.2f}")


def mongo_delete_records(collection_data:list, collection_name:str):
    """ Delete records from a mongo collection.

    Args:
        collection_data: List of records.
        collection_name: Collection name.
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


def mongo_create_index(collection_name:str, index_name:str):
    """ Create index on a mongo collection if unique.

    Args:
        collection_name: Collection name.
        index_name: Index to create.
    """

    if not COLLECTIONS_CONFIG[collection_name].get("unique"):
        # logger.warn("Collection {} not defined as unique: index not created");
        return

    create_index_timer = timer()
    mongo_client = MongoClient(os.getenv("MONGO_URI"))
    mongo_inpi = mongo_client[INPI_DB]
    collection = mongo_inpi[collection_name]

    # logger.info(f"Collection {collection_name} start indexing by '{index_name}'")
    try:
        collection.create_index(index_name, unique=True)
    except Exception as error:
        logger.error(f"Error while creating an unique index:\n{error}")

    mongo_client.close()

    logger.info(f"Collection {collection_name} indexed in {(timer() - create_index_timer):.2f}")


@retry(delay=50, tries=3)
def mongo_import_collection(collection_name:str, collection_data:list):
    """ Import collection data into mongo db.

    Args:
        collection_name: Collection name.
        collection_data: List of records.
    """
    if not collection_data:
        logger.warn(f"Collection {collection_name} not imported. Collection data is empty")
        return

    logger.debug(f"Collection {collection_name} contains {len(collection_data)} records to add")
    # logger.debug(f"{collection}")

    # Delete from mongo
    if collection_name not in mongo_collections_find_field("with_history"):
        mongo_delete_records(collection_data, collection_name)

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


def mongo_import(data:list, collections_names:list):
    """Import data into mongo db.

    Args:
        data: List of records.
        collections_names: List of collections.
    """

    if not data:
        logger.warn("Import data is empty!")
        return

    collections_timer = timer()
    logger.info(f"Start mongo import of {collections_names}")

    # Get collection records from data
    for collection_name in collections_names:
        logger.debug(f"Collection {collection_name} start loading")
        collection_data = []

        # Get records
        for record in data:
            collection_record = record.get(collection_name)
            if collection_record:
                if isinstance(collection_record, list):
                    collection_data.extend(collection_record)
                else:
                    collection_data.append(collection_record)

        # Import collection
        mongo_import_collection(collection_name, collection_data)

        # Create index
        mongo_create_index(collection_name, INPI_INDEX)

    logger.info(f"Collections imported in {(timer() - collections_timer):.2f}")
