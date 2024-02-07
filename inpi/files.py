import os
import csv
import glob
from pathlib import Path
from collections import defaultdict

from utils.utils import chunks, remove_prefix

from application.server.main.logger import get_logger

logger = get_logger(__name__)

DATA_PATH = os.getenv("MOUNTED_VOLUME_TEST")
INPI_PATH = os.path.join(DATA_PATH, "INPI")
LOAD_CSV_PATH = os.path.join(DATA_PATH, "to_load.csv")
XML_PATTERN = "*.xml"
NEW_PATTERN = "FR_FRNEW"


def files_remove_csv():
    """Remove csv on disk."""
    os.remove(LOAD_CSV_PATH)


def files_get_chunks(files, max_size):
    """Get chunks from list of files.

    Args:
        files (_type_): _description_
        max_size (_type_): _description_

    Returns:
        _type_: _description_
    """
    files_size = len(files)

    if files_size <= max_size:
        return [files]

    chunks_number = (files_size // max_size) + 1
    chunks_list = list(chunks(files, chunks_number))
    chunks_list_size = [len(chunk) for chunk in chunks_list]

    logger.debug(f"{files_size} files split in {chunks_number} chunks ({chunks_list_size})")

    return chunks_list


def files_sort(files):
    """Sort files NEW >> AMD.

    Args:
        files (_type_): _description_
    """

    if len(files) > 1:
        new_files = sorted([file for file in files if NEW_PATTERN in file])
        amd_files = sorted([file for file in files if NEW_PATTERN not in file])
        files = new_files + amd_files

    return files


def files_remove_duplicates(files):
    """Remove duplicates from files and keep must recent ones.

    Args:
        files (_type_): _description_
    """
    # Create dictionnary of occurences from files
    tally = defaultdict(list)
    for index, file in enumerate(files):
        file_name = remove_prefix(Path(file).stem, "FR")
        tally[file_name].append(index)

    # Keep last occurence of file
    files_last = [files[indexes[-1]] for _, indexes in tally.items()]

    if len(files) == len(files_last):
        logger.debug(f"No duplicates found in {len(files)} files")
    else:
        logger.debug(f"Removed {len(files) - len(files_last)} duplicate files")

    return files_last


def files_import_from_years(path, years=None):
    """Get all xml files from path.

    Args:
        path (_type_): _description_
        years (_type_, optional): _description_. Defaults to None.

    Returns:
        _type_: _description_
    """
    files = []

    # Get all files
    if not years:
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

    return files


def files_import_from_csv():
    """Import files list from csv."""
    files = []

    # Check file exist
    if not os.path.isfile(LOAD_CSV_PATH):
        logger.warn(f"[DISK] File {LOAD_CSV_PATH} not found")
        return files

    # Read file
    with open(LOAD_CSV_PATH) as f:
        reader = csv.reader(f)
        files = [
            line[0] if line[0].startswith(INPI_PATH) else os.path.join(INPI_PATH, line[0]) for line in reader
        ]  # Only one column

    logger.debug(f"[CSV] Found {len(files)} xml files")

    return files


def files_import(remove_duplicates=True, force=False, force_years=None):
    """Import files from list on disk and from years if forced

    Args:
        remove_duplicates (bool, optional): _description_. Defaults to True.
        force (bool, optional): _description_. Defaults to False.
        force_years (list, optional): _description_. Defaults to [].

    Returns:
        _type_: _description_
    """

    if force:
        files += files_import_from_years(INPI_PATH, force_years)  # Import from years
    else:
        files = files_import_from_csv()  # Import from csv

    if not files:
        return files

    # Sort files
    files = files_sort(files)

    # Remove duplicates
    if remove_duplicates:
        files = files_remove_duplicates(files)

    return files
