import os
from timeit import default_timer as timer

from inpi.extract import extract
from inpi.files import files_import, files_get_chunks, files_remove_csv
from inpi.mongo import mongo_delete_collections, mongo_import, mongo_collections_find_field

from application.server.main.logger import get_logger

logger = get_logger(__name__)

DATA_PATH = os.getenv("MOUNTED_VOLUME_TEST")
INPI_PATH = os.path.join(DATA_PATH, "INPI")
INPI_DB = "inpi"
INPI_INDEX = "publication-number"
CHUNK_SIZE = 10000


def mongo_load(with_history=False, force=False, force_years=None, reset_mongo=False):
    """Load files in mongo db

    Args:
        force (bool, optional): _description_. Defaults to False.
    """

    os.chdir(INPI_PATH)

    load_timer = timer()

    logger.info(f"Start mongo load")

    # Get files
    remove_duplicates = not with_history
    files_to_add = files_import(remove_duplicates, force, force_years)
    if not files_to_add:
        logger.warn("Mongo load aborted: no files found")
        return

    # Get collections to extract
    extract_collections = mongo_collections_find_field("with_history", include=with_history)
    logger.debug(f"Collections to extract {extract_collections} (with_history={with_history})")

    # Reset mongo
    if reset_mongo:
        mongo_delete_collections(extract_collections)

    # Chunk files
    for chunk_to_add in files_get_chunks(files_to_add, CHUNK_SIZE):
        chunk_timer = timer()

        # Extract data
        data_to_add = extract(chunk_to_add, extract_collections, show_progress=True)

        # Load mongo db
        mongo_import(data_to_add, extract_collections)

        logger.debug(f"Chunk loaded in {(timer() - chunk_timer):.2f}")

    # Delete csv from disk
    # files_remove_csv()

    logger.info(f"Mongo loaded in {(timer() - load_timer):.2f}")
    return
