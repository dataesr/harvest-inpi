import os

from inpi import *

from application.server.main.logger import get_logger

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
    ftp_inpi.loading()
    logger.debug("chargement de la dernière version complète de la DB de l'INPI")

