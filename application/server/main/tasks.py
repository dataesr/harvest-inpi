import os

from application.server.main.logger import get_logger
from inpi import p00_ftp_inpi, p01_unzip_inpi

logger = get_logger(__name__)

DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')


def create_task_clean(args):
    os.system(f'rm -rf {DATA_PATH}')


def create_task_all(args):
    # if args.get('recuperation', True):
    #     create_recuperation()
    if args.get('harvest-inpi', True):
        harvest_inpi()


def harvest_inpi():
    p00_ftp_inpi.loading()
    logger.debug("chargement de la dernière version complète de la DB de l'INPI")
    p01_unzip_inpi.unzip()
    logger.debug("dezippage des fichiers zippés, chargement dans ObjectStorage et base mongo INPI")

