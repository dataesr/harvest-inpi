import os

from application.server.main.logger import get_logger
from inpi import p00_ftp_inpi, p01_unzip_inpi
from inpi.load import mongo_load
from inpi.files import files_remove_csv
from inpi.mongo import mongo_delete_duplicates

logger = get_logger(__name__)

DATA_PATH = os.getenv("MOUNTED_VOLUME_TEST")


def task_disk_clean(args):
    os.system(f"rm -rf {DATA_PATH}")


def task_remove_csv(args):
    files_remove_csv()


def task_download_inpi(args):
    # Load INPI database from ftp
    p00_ftp_inpi.loading()


def task_unzip_inpi(args):
    # Unzip files
    p01_unzip_inpi.unzip()


def task_download_and_unzip_inpi(args):
    # Load INPI database from ftp
    p00_ftp_inpi.loading()

    # Unzip files
    p01_unzip_inpi.unzip()


def task_mongo_load(args):
    # Extract and load mongo
    mongo_load()


def task_mongo_load_with_history(args):
    # Extract and load mongo
    mongo_load(with_history=True)


def task_mongo_load_force(args):
    # Extract and load mongo
    mongo_load(force=True, force_years=args.get("force_years"))


def task_mongo_load_force_with_history(args):
    # Extract and load mongo
    mongo_load(with_history=True, force=True, force_years=args.get("force_years"))


def task_mongo_reload(args):
    # Extract and load mongo
    mongo_load(reset_mongo=True)


def task_mongo_reload_with_history(args):
    # Extract and load mongo
    mongo_load(with_history=True, reset_mongo=True)


def task_mongo_reload_force(args):
    # Reset mongo, extract and load mongo
    mongo_load(force=True, force_years=args.get("force_years"), reset_mongo=True)


def task_mongo_reload_force_with_history(args):
    # Reset mongo, extract and load mongo
    mongo_load(with_history=True, force=True, force_years=args.get("force_years"), reset_mongo=True)


def task_harvest_inpi(args):
    # Load INPI database from ftp
    p00_ftp_inpi.loading()

    # Unzip files
    p01_unzip_inpi.unzip()

    # Extract and load mongo
    mongo_load()


def task_mongo_delete_duplicates(args):
    # Remove exact duplicates from mongo collections
    mongo_delete_duplicates(collections_names=args.get("collections_names"))
