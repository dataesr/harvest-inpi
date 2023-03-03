#!/usr/bin/env python
# coding: utf-8

"""
This program unzips only the XML files named with the patent publication number.
Folder structures evolve and cqn vary from a week to the next so this means that different strategies are needed
to unzip only the wanted files.
"""

import glob
import os
import zipfile
import re
import shutil
import tarfile

# directory where the files are
DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')


def fan(fle: str) -> str:
    """
    Get year from file name
    :param fle:
    :return:
    """
    nom = fle.replace(".zip", "")
    nom = nom.split("_")
    annee = nom[len(nom) - 2]

    return annee


def select_xml(lfiles: list) -> list:
    """
    Select XML files which name only contains digits
    :param lfiles:
    :return:
    """
    lfiles = [re.findall(r".+\d+\.xml", it) for it in lfiles]
    lfiles = [it for it in lfiles if it]
    lfiles = list(map(lambda a: a[0], lfiles))
    return lfiles


def sfiles(zpr) -> list:
    """
    Select files within the zip we want to extract
    :param zpr:
    :return:
    """
    lfiles = zpr.namelist()
    lfiles = select_xml(lfiles)
    return lfiles


def biblio():
    """
    Unzip folder with older patents
    :return:
    """
    list_dir = os.listdir(DATA_PATH)
    list_dir = list(set(map(lambda a: re.sub(r"\.zip", "", a), list_dir)))
    list_dir.sort()

    if 'Biblio_FR_Stock.tar' in list_dir:
        my_tar = tarfile.open('Biblio_FR_Stock.tar')
        my_tar.extractall('.')
        my_tar.close()
        os.remove('Biblio_FR_Stock.tar')


def remove_extras(pths: list):
    """
    Remove CCP & schemas
    :param pths:
    :return:
    """
    for pth in pths:
        list_dir = os.listdir(pth)
        rem = list(map(lambda a: re.findall(r"FR_FRCCPST36_.+", a), list_dir))
        rem = [item for item in rem if item]
        rem = list(map(lambda a: a[0], rem))
        if rem:
            for i in rem:
                os.remove(pth + i)

        rem = list(map(lambda a: re.findall(r"Schema_.+", a), list_dir))
        rem = [item for item in rem if item]
        rem = list(map(lambda a: a[0], rem))
        if rem:
            for i in rem:
                shutil.rmtree(pth + i)


def id_fld1(pths: list) -> dict:
    """
    Older folders to apply the first types of unzipping
    :param pths:
    :return:
    """
    dco = {}

    for path in pths:
        fl = []
        lzip = glob.glob(f"{path}*.zip")
        if lzip:
            for file in lzip:
                nfile = file.replace(path, "")
                nfile = nfile.replace(".zip", "")
                with zipfile.ZipFile(file, 'r') as zip_ref:
                    lfiles = sfiles(zip_ref)
                    if lfiles:
                        fl.append({nfile: lfiles})
            if fl:
                dco[path] = fl

    return dco


def select_unzip(dco: dict, clfs: list) -> (dict, dict, dict):
    """
    Split older folders according to their file structure (doc in first place in path or not, multiple paths
    with zip files
    :param dco:
    :param clfs:
    :return:
    """
    dc_dc = {}
    dc_rste = {}
    dc_mlti = {}
    for clf in clfs:
        doc = []
        reste = []
        multi = []
        for dic in dco[clf]:
            cl = list(dic.keys())[0]
            liste = [re.sub(r"^/", "", item).split("/")[0] for item in dic[cl]]
            valeurs = list(set(liste))
            if len(valeurs) == 1:
                if valeurs[0] == "doc":
                    doc.append(cl)
                else:
                    reste.append(cl)
            else:
                multi.append(cl)
            if doc:
                dc_dc[clf] = doc
            if reste:
                dc_rste[clf] = reste
            if multi:
                dc_mlti[clf] = multi

    return dc_dc, dc_rste, dc_mlti


def select_missing(dco: dict, clfs: list) -> dict:
    """
    Select missing files within dictionary of the older folders
    :param dco:
    :param clfs:
    :return:
    """
    dc_mis = {}
    for clf in clfs:
        lis = []
        list_dir = os.listdir(clf)
        list_dir2 = list(map(lambda a: a.replace(".zip", ""), list_dir))
        for dic in dco[clf]:
            cl = list(dic.keys())[0]
            lis.append(cl)
        mq = list(set(list_dir2).difference(set(lis)))
        if mq:
            dc_mis[clf] = mq

    return dc_mis


def unzip_doc(dc_dc: dict):
    """
    Unzipping of folders where single path that starts with doc
    :param dc_dc:
    :return:
    """
    clfs = list(dc_dc.keys())
    for clf in clfs:
        for item in dc_dc[clf]:
            with zipfile.ZipFile(clf + item + ".zip", 'r') as zip_ref:
                zip_ref.extractall(clf + item)
                fol = glob.glob(f"{clf}{item}/doc/*.zip")
                for fl in fol:
                    with zipfile.ZipFile(fl, 'r') as zp_ref:
                        lfiles = sfiles(zp_ref)
                        for fil in lfiles:
                            zp_ref.extract(fil, clf + item)
            cont = os.listdir(clf + item)
            for c in cont:
                if os.path.isdir(clf + item + "/" + c):
                    shutil.rmtree(clf + item + "/" + c)
                else:
                    lfiles = select_xml(cont)
                    if c not in lfiles:
                        if os.path.isfile(c):
                            os.remove(clf + item + "/" + c)
                    if c == "Volumeid":
                        os.remove(clf + item + "/" + c)
            os.remove(clf + item + ".zip")


def unzip_reste(dc_rste: dict):
    """
    Unzipping of folders where single path which does not start with doc
    :param dc_rste:
    :return:
    """
    clfs = list(dc_rste.keys())
    for clf in clfs:
        for item in dc_rste[clf]:
            with zipfile.ZipFile(clf + item + ".zip", 'r') as zip_ref:
                zip_ref.extractall(clf + item)
                fol = glob.glob(f"{clf}{item}/{item}/doc/*.zip")
                for fl in fol:
                    with zipfile.ZipFile(fl, 'r') as zp_ref:
                        lfiles = sfiles(zp_ref)
                        for fil in lfiles:
                            zp_ref.extract(fil, clf + item)
            cont = os.listdir(clf + item)
            for c in cont:
                if os.path.isdir(clf + item + "/" + c):
                    shutil.rmtree(clf + item + "/" + c)
                else:
                    lfiles = select_xml(cont)
                    if c not in lfiles:
                        if os.path.isfile(c):
                            os.remove(clf + item + "/" + c)
                    if c == "Volumeid":
                        os.remove(clf + item + "/" + c)
            os.remove(clf + item + ".zip")


def unzip_multi(dc_mlti: dict):
    """
    Unzip older folders with multiple paths with zip files
    :param dc_mlti:
    :return:
    """
    clfs = list(dc_mlti.keys())
    for clf in clfs:
        for item in dc_mlti[clf]:
            with zipfile.ZipFile(clf + item + ".zip", 'r') as zip_ref:
                zip_ref.extractall(clf + item)
                fol = glob.glob(f"{clf}{item}/*.zip")
                if fol:
                    for fl in fol:
                        with zipfile.ZipFile(fl, 'r') as zp_ref:
                            lfiles = sfiles(zp_ref)
                            for fil in lfiles:
                                zp_ref.extract(fil, clf + item)
                        os.remove(fl)
            if os.path.isdir(f"{clf}{item}/doc/"):
                fol = glob.glob(f"{clf}{item}/doc/*.zip")
                for fl in fol:
                    with zipfile.ZipFile(fl, 'r') as zip_ref:
                        lfiles = sfiles(zip_ref)
                        for fil in lfiles:
                            zip_ref.extract(fil, clf + item)
            cont = os.listdir(clf + item)
            for c in cont:
                if os.path.isdir(clf + item + "/" + c):
                    shutil.rmtree(clf + item + "/" + c)
                else:
                    lfiles = select_xml(cont)
                    if c not in lfiles:
                        if os.path.isfile(c):
                            os.remove(clf + item + "/" + c)
                    if c == "Volumeid":
                        os.remove(clf + item + "/" + c)
            os.remove(clf + item + ".zip")


def unzip_missing(dc_mis: dict):
    """
    Unzip missing files within dictionary of the older folders
    :param dc_mis:
    :return:
    """
    clfs = list(dc_mis.keys())
    for clf in clfs:
        dos = glob.glob(f"{clf}*.zip")
        for item in dos:
            with zipfile.ZipFile(item, 'r') as zip_ref:
                lfiles = sfiles(zip_ref)
                for fil in lfiles:
                    zip_ref.extract(fil, clf)
                    folder = item.replace(".zip", "")
                    if not os.path.isdir(folder):
                        os.makedirs(folder)
                    shutil.move(clf + fil, folder)
                    if os.path.isdir(f"{folder}/doc/"):
                        shutil.rmtree(f"{folder}/doc/")
                os.remove(item)


def select_newer(pths: list, dco: dict):
    """
    Newer folders to apply the second type of unzipping
    :param pths:
    :param dco:
    :return:
    """
    pths2 = list(set(pths).difference(set(dco.keys())))
    pths2.sort()

    pths3 = []

    for pth in pths2:
        lzip = glob.glob(f"{pth}*.zip")
        if len(lzip) > 0:
            pths3.append(pth)

    return pths3


def unzip_newer(fle: str):
    """
    Unzip method for newer folders
    :param fle:
    :return:
    """
    with zipfile.ZipFile(fle, 'r') as zip_ref:
        lfiles = sfiles(zip_ref)
        for fil in lfiles:
            zip_ref.extract(fil)
            flder = fle.replace(".zip", "")
            shutil.move(fil, flder)
            shutil.rmtree(f"{flder}/doc/")
    os.remove(fle)


def unzip():
    os.chdir(DATA_PATH)

    biblio()

    paths = []
    folders = os.listdir(DATA_PATH)
    for folder in folders:
        if os.path.isdir(folder):
            paths.append(DATA_PATH + folder + "/")

    paths.sort()

    remove_extras(paths)

    #####################################################################################################

    dico = id_fld1(paths)

    clefs = list(dico.keys())

    dc_doc, dc_reste, dc_multi = select_unzip(dico, clefs)

    #####################################################################################################

    dc_missing = select_missing(dico, clefs)

    #####################################################################################################

    unzip_doc(dc_doc)

    #####################################################################################################

    unzip_reste(dc_reste)

    #####################################################################################################

    unzip_multi(dc_multi)

    #####################################################################################################

    unzip_missing(dc_missing)

    #####################################################################################################

    paths_new = select_newer(paths, dico)

    for path in paths_new:
        dos = glob.glob(f"{path}*.zip")
        for item in dos:
            unzip_newer(item)

    #####################################################################################################

    dossiers_zip = glob.glob(f"*.zip")

    for dos in dossiers_zip:
        an = fan(dos)
        nom = dos.replace(".zip", "")
        if an in paths:
            ldos = os.listdir(DATA_PATH + an + "/")
            if nom in ldos:
                dossiers_zip.remove(dos)

    for file in dossiers_zip:
        annee = fan(file)
        unzip_newer(file)
        fannee = DATA_PATH + annee
        folder = file.replace(".zip", "")
        if not os.path.isdir(fannee):
            os.makedirs(fannee)
        shutil.move(folder, fannee)
