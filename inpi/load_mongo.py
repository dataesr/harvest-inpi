import os
import glob
from pymongo import MongoClient
from application.server.main.logger import get_logger
from inpi.extract import extract
from utils.utils import to_jsonl
from timeit import default_timer as timer

logger = get_logger(__name__)

DATA_PATH = os.getenv("MOUNTED_VOLUME_TEST")
COLLECTIONS_NAMES = ["dic_pn", "pref"]
XML_PATTERN = "*.xml"


def get_xml_files(path, years=None):
    """Get all xml files from path

    Args:
        path (_type_): _description_
        years (_type_, optional): _description_. Defaults to None.
    """
    files = []

    # Get all files
    if years is None:
        path = os.path.join(path, "**", XML_PATTERN)

        for file in glob.glob(path, recursive=True):
            files.append(file)

    # Get specific years files
    else:
        for year in years:
            path = os.path.join(path, year, "**", XML_PATTERN)
            year_files = []

            for file in glob.glob(path, recursive=True):
                year_files.append(file)

            logger.debug(f"[{year}] Found {len(year_files)} xml files")
            files += year_files

    logger.debug(f"[TOTAL] Found {len(files)} xml files")
    return files


def to_mongo(data):
    """_summary_

    Args:
        data (_type_): _description_
    """

    if len(data) == 0:
        logger.debug("Data empty, return")
        return

    # Get collection records from data
    for collection_name in COLLECTIONS_NAMES:
        logger.debug(f"Collection {collection_name} start loading")
        collection = []
        for record in data:
            if collection_name in record:
                collection += record[collection_name]

        # Import collection
        if collection:
            collection_timer = timer()
            logger.debug(f"Collection {collection_name} contains {len(collection)} records to add")
            # logger.debug(f"{collection}")

            # Save a json
            output_json = os.path.join(DATA_PATH, "INPI", f"{collection_name}.jsonl")
            logger.debug(f"Collection {collection_name} output json : {output_json}")
            to_jsonl(collection, output_json, "w")

            # Get mongo database
            mongo_client = MongoClient(os.getenv("MONGO_URI"))
            logger.debug(f"Mongo DB {mongo_client.list_database_names()}")

            # Import to mongo (mongoimport)
            mongoimport = f'mongoimport --numInsertionWorkers 2 --uri "mongodb://apps:hZeW2iH8JgG09PKuva6m@node1-32c5d10cc5b28490.database.cloud.ovh.net/inpi" --collection {collection_name} --file {output_json}'
            logger.debug(f"Current directory {os.getcwd()}")
            logger.debug(f"{mongoimport}")
            logger.debug(f"{output_json} exist : {os.path.exists(output_json)}")
            os.system(f"echo $PWD")
            os.system(f"echo $(ls INPI)")
            os.system(mongoimport)
            # os.remove(output_json)

            mongo_client.close()
            logger.debug(f"Collection {collection_name} loaded in {timer() - collection_timer}")


def load_mongo(force=False, years=None):
    """Load files in mongo db

    Args:
        force (bool, optional): _description_. Defaults to False.
    """

    PATH = os.path.join(DATA_PATH, "INPI/")
    os.chdir(PATH)

    files_to_add = []  # files to add

    # @TODO: Check years is a list

    if force:
        files_to_add = get_xml_files(PATH, years)
    else:
        logger.error("Force=false not developped yet")

    # Prepare data
    data_to_add = extract(files_to_add)

    # Load mongo db
    to_mongo(data_to_add)
