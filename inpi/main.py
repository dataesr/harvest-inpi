import os
from timeit import default_timer as timer

from inpi.extract import extract
from inpi.files import files_import, files_get_chunks, files_remove_disk
from inpi.mongo import mongo_delete_collections, mongo_import, mongo_create_index

from application.server.main.logger import get_logger

logger = get_logger(__name__)

DATA_PATH = os.getenv("MOUNTED_VOLUME_TEST")
INPI_PATH = os.path.join(DATA_PATH, "INPI")
INPI_DB = "inpi"
INPI_INDEX = "publication-number"
CHUNK_SIZE = 10000


def mongo_load(remove_duplicates=True, force=False, force_years=None, reset_mongo=False):
    """Load files in mongo db

    Args:
        force (bool, optional): _description_. Defaults to False.
    """

    os.chdir(INPI_PATH)

    load_timer = timer()

    logger.info(f"Start mongo load")

    # Reset mongo
    if reset_mongo:
        mongo_delete_collections()

    # Get files
    files_to_add = files_import(remove_duplicates, force, force_years)

    # Chunk files
    for chunk_to_add in files_get_chunks(files_to_add, CHUNK_SIZE):
        chunk_timer = timer()

        # Extract data
        data_to_add = extract(chunk_to_add, show_progress=True)

        # Load mongo db
        mongo_import(data_to_add)

        # Create index
        mongo_create_index("publication", INPI_INDEX)
        logger.debug(f"Chunk loaded in {(timer() - chunk_timer)}")

        return

    # Delete files list from disk
    files_remove_disk()

    logger.info(f"Mongo loaded in {(timer() - load_timer):.2f}")
    return
