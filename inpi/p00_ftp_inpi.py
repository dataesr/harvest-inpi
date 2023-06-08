#!/usr/bin/env python
# coding: utf-8

import ftplib
import os
import re

# directory where the files are
DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')
# DATA_PATH = "/run/media/julia/DATA/INPI/"


def _is_ftp_dir(ftp_handle, name, guess_by_extension=True):
    """ simply determines if an item listed on the ftp server is a valid directory or not """

    # if the name has a "." in the fourth to last position, it's probably a file extension
    # this is MUCH faster than trying to set every file to a working directory, and will work 99% of time.
    if guess_by_extension is True:
        if len(name) >= 4:
            if name[-4] == '.':
                return False

    original_cwd = ftp_handle.pwd()  # remember the current working directory
    try:
        ftp_handle.cwd(name)  # try to set directory to new name
        ftp_handle.cwd(original_cwd)  # set it back to what it was
        return True

    except ftplib.error_perm as e:
        print(e)
        return False

    except Exception as e:
        print(e)
        return False


def _make_parent_dir(fpath):
    """ ensures the parent directory of a filepath exists """
    dirname = os.path.dirname(fpath)
    while not os.path.exists(dirname):
        try:
            os.makedirs(dirname)
            print("created {0}".format(dirname))
        except OSError as e:
            print(e)
            _make_parent_dir(dirname)


def _download_ftp_file(ftp_handle, name, dest, overwrite):
    """ downloads a single file from an ftp server """
    _make_parent_dir(dest.lstrip("/"))
    if not os.path.exists(dest) or overwrite is True:
        try:
            with open(dest, 'wb') as f:
                ftp_handle.retrbinary("RETR {0}".format(name), f.write)
            print("downloaded: {0}".format(dest))
        except FileNotFoundError:
            print("FAILED: {0}".format(dest))
    else:
        print("already exists: {0}".format(dest))


def _file_name_match_patern(pattern, name):
    """ returns True if filename matches the pattern"""
    if pattern is None:
        return True
    else:
        return bool(re.match(pattern, name))


def _mirror_ftp_dir(ftp_handle, name, overwrite, guess_by_extension, pattern):
    """ replicates a directory on an ftp server recursively """
    for item in ftp_handle.nlst(name):
        if _is_ftp_dir(ftp_handle, item, guess_by_extension):
            _mirror_ftp_dir(ftp_handle, item, overwrite, guess_by_extension, pattern)
        else:
            if _file_name_match_patern(pattern, name):
                _download_ftp_file(ftp_handle, item, item, overwrite)
            else:
                # quietly skip the file
                pass


def download_ftp_tree(ftp_handle, path, destination, pattern=None, overwrite=False, guess_by_extension=True):
    """
    Downloads an entire directory tree from an ftp server to the local destination
    :param ftp_handle: an authenticated ftplib.FTP instance
    :param path: the folder on the ftp server to download
    :param destination: the local directory to store the copied folder
    :param pattern: Python regex pattern, only files that match this pattern will be downloaded.
    :param overwrite: set to True to force re-download of all files, even if they appear to exist already
    :param guess_by_extension: It takes a while to explicitly check if every item is a directory or a file.
        if this flag is set to True, it will assume any file ending with a three character extension ".???" is
        a file and not a directory. Set to False if some folders may have a "." in their names -4th position.
    """
    path = path.lstrip("/")
    original_directory = os.getcwd()  # remember working directory before function is executed
    os.chdir(destination)  # change working directory to ftp mirror directory

    _mirror_ftp_dir(
        ftp_handle,
        path,
        pattern=pattern,
        overwrite=overwrite,
        guess_by_extension=guess_by_extension)

    os.chdir(original_directory)  # reset working directory to what it was before function exec


def list_files(pattern: str, liste: list) -> list:
    fil = [re.findall(pattern, item) for item in liste]
    fil = [item for item in fil if item]
    fil = list(map(lambda a: a[0], fil))

    return fil


def remove_zip(liste: list) -> list:
    liste2 = list(map(lambda a: re.sub(r"\.zip", "", a), liste))

    return liste2


def loading_file(ftp_handle, file):
    with open(f"{DATA_PATH}/INPI/{file}", 'wb') as f:
        ftp_handle.retrbinary(f"RETR {file}", f.write)


def loading():
    # os.chdir(DATA_PATH)
    os.system(f'mkdir -p {DATA_PATH}/INPI')
    path = os.path.join(DATA_PATH, "INPI/")
    os.chdir(path)
    # check which directories are already present
    present = os.listdir()

    # connect to the FTP server
    ftp_server = ftplib.FTP('www.inpi.net', os.getenv('USERNAME_INPI'), os.getenv('PWD_INPI'))

    # list all the elements available
    liste = []
    ftp_server.retrlines('LIST', liste.append)

    files = ftp_server.nlst()

    # keep only the yearly forlders
    year_list = list_files(r"^\d{4}$", files)

    # select which folders to load by comparing what we already have and what is available on the server
    year_load = []
    for item in year_list:
        if item not in present:
            year_load.append(item)

    # load the folders we are interested in
    if len(year_load) > 0:
        for i in year_load:
            download_ftp_tree(ftp_server, i, f"{path}", pattern=None, overwrite=False,
                              guess_by_extension=True)

    # load the folder with data prior 2017
    pre_2017 = []
    for i in range(2010, 2017):
        if str(i) not in present:
            pre_2017.append(str(i))

    if len(pre_2017) > 0 and "Biblio_FR_Stock.tar" not in present:
        biblio = list_files(r"^Biblio.+", files)[0]
        loading_file(ftp_server, biblio)

    # select files that start with FR_FR (current year usually - folders by week)
    rest_years = list_files(r"^FR_FR.+", files)

    rest_present = list_files(r"^FR_FR.+", present)
    rest_present = remove_zip(rest_present)

    rest_years2 = remove_zip(rest_years)

    # load them if we don't already have them in the folder from the closest year

    global annee

    if rest_years2:
        annee = max(list(set(list_files(r"\d{4}", rest_years2))))

    global anmax
    if present:
        anmax = max(present)
    if "anmax" in globals():
        if not rest_present:
            if rest_years2:
                for nom in rest_years2:
                    print(os.listdir(f"{path}"))
                    if nom not in os.listdir(f"{path}"):
                        print(nom)
                        loading_file(ftp_server, nom + ".zip")
        else:
            if rest_years2:
                for item in rest_years2:
                    if item in rest_present:
                        loading_file(ftp_server, item + ".zip")
    else:
        if not rest_present:
            if rest_years2:
                for nom in rest_years2:
                    if os.path.exists(f"{path}{annee}"):
                        if nom not in os.listdir(f"{path}{annee}"):
                            print(nom)
                            loading_file(ftp_server, nom + ".zip")
                    else:
                        loading_file(ftp_server, nom + ".zip")
        else:
            if rest_years2:
                for item in rest_years2:
                    if item in rest_present:
                        loading_file(ftp_server, item + ".zip")


    ftp_server.quit()
