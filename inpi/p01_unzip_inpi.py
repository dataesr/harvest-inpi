#!/usr/bin/env python
# coding: utf-8

import glob
import os
import zipfile
import re
import shutil
import tarfile
import boto3
import pandas as pd
import logging
import threading
import sys
import concurrent.futures

from inpi import p02_lecture_xml as p02
from pymongo import MongoClient

# directory where the files are
DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')






def get_logger(name):
    """
    This function helps to follow the execution of the parallel computation.
    """
    loggers = {}
    if name in loggers:
        return loggers[name]
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    fmt = '%(asctime)s - %(threadName)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(fmt)

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    loggers[name] = logger
    return loggers[name]


def subset_df(df: pd.DataFrame) -> dict:
    """
    This function divides the initial df into subsets which represent ca. 10 % of the original df.
    The subsets are put into a dictionary with 10-11 pairs key-value.
    Each key is the df subset name and each value is the df subset.
    """
    prct10 = int(round(len(df) * 10 / 100, 0))
    dict_nb = {}
    deb = 0
    fin = prct10
    dict_nb["df1"] = df.iloc[deb:fin, :]
    deb = fin
    dixieme = 10 * prct10
    reste = (len(df) - dixieme)
    fin_reste = len(df) + 1
    for i in range(2, 11):
        fin = (i * prct10 + 1)
        dict_nb["df" + str(i)] = df.iloc[deb:fin, :]
        if reste > 0:
            if len(df.iloc[fin: fin_reste, :]) > 0:
                dict_nb["reste"] = df.iloc[fin: fin_reste, :]
        deb = fin

    return dict_nb


def req_xml_aws(df: pd.DataFrame):
    session = boto3.Session(region_name='gra', aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"))
    conn = session.client("s3", aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                          aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                          endpoint_url=os.getenv("ENDPOINT_URL"))
    print("Connexion AWS S3", flush=True)
    logger = get_logger(threading.current_thread().name)
    logger.info("start query xml aws")
    for _, r in df.iterrows():
        try:
            response = conn.upload_file(r.dirpath, "inpi-xmls", f"{r.prefix}/{r.file}")
            print(f"{r.prefix}/{r.file} added in inpi-xmls", flush=True)
        except boto3.exceptions.S3UploadFailedError as error:
            print(error.response, flush=True)
            raise error

    logger.info("end query xml aws")


def res_futures(dict_nb: dict, query):
    """
    This function applies the query function on each subset of the original df in a parallel way
    It takes a dictionary with 10-11 pairs key-value. Each key is the df subset name and each value is the df subset
    It returns a df with the IdRef.
    """
    global jointure
    res = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=11, thread_name_prefix="thread") as executor:
        # Start the load operations and mark each future with its URL
        future_to_req = {executor.submit(query, df): df for df in dict_nb.values()}
        for future in concurrent.futures.as_completed(future_to_req):
            req = future_to_req[future]
            try:
                data = future.result()
                res.append(data)
                jointure = pd.concat(res)
            except Exception as exc:
                print('%r generated an exception: %s' % (req, exc), flush=True)


def mongo_fill(df: pd.DataFrame):
    logger = get_logger(threading.current_thread().name)
    logger.info("start loading mongo")
    for fil in set(df["file"]):
        with open(fil, "r") as f:
            data = f.read()
        p02.update_db_new(fil, data)
    logger.info("end loading mongo")


def unzip():
    """
    We keep only the XML files with information on patent, no CCP, no schema, no drawing...
    Across time, folder and file structures have evolved. This means that the unzipping process needs to take into these
    changes; some files are directly inside a weekly folder, some are inside a folder with no weekly information,
    some are inside another folder named doc...
    :return:
    """
    PATH = os.path.join(DATA_PATH, "INPI/")
    os.chdir(PATH)

    # list all the folders
    list_dir = os.listdir(PATH)
    # remove .zip from the name of the folders and get a list of the unique names
    list_dir = list(set(map(lambda a: re.sub(r"\.zip", "", a), list_dir)))
    list_dir.sort()
    dir2 = os.listdir(PATH)

    # unzip data pre-2017 if not already done
    if 'Biblio_FR_Stock.tar' in list_dir:
        my_tar = tarfile.open('Biblio_FR_Stock.tar')
        my_tar.extractall('.')
        my_tar.close()
        os.remove('Biblio_FR_Stock.tar')

    # get all the full data paths
    new_complete = []
    paths = []
    dic_pref_fil = {"dirpath": [], "prefix": [], "file": []}
    folders = os.listdir(PATH)
    for folder in folders:
        if os.path.isdir(folder):
            paths.append(PATH + folder + "/")

    paths.sort()

    # remove CCP and schemas from paths
    for path in paths:
        list_dir = os.listdir(path)
        rem = list(map(lambda a: re.findall(r"FR_FRCCPST36_.+", a), list_dir))
        rem = [item for item in rem if item]
        rem = list(map(lambda a: a[0], rem))
        if rem:
            for i in rem:
                os.remove(path + i)

        rem = list(map(lambda a: re.findall(r"Schema_.+", a), list_dir))
        rem = [item for item in rem if item]
        rem = list(map(lambda a: a[0], rem))
        if rem:
            for i in rem:
                shutil.rmtree(path + i)

    #####################################################################################################

    # get all the files available inside the zipped folders and put them into a dictionary with year as a key
    dico = {}
    all_files = []

    for path in paths:
        fl = []
        lp = len(path) - 1
        pth = path[0:lp]
        lzip = glob.glob(f"{path}*.zip")
        if lzip:
            for file in lzip:
                nfile = file.replace(path, "")
                nfile = nfile.replace(".zip", "")
                with zipfile.ZipFile(file, 'r') as zip_ref:
                    lfiles = zip_ref.namelist()
                    lfiles = [re.findall(".+\.zip", item) for item in lfiles]
                    lfiles = [item for item in lfiles if item]
                    lfiles = list(map(lambda a: a[0], lfiles))
                    if lfiles:
                        all_files.append(lfiles)
                        fl.append({nfile: lfiles})
            if fl:
                dico[path] = fl

    # for each full path by year, check which strategy is better suited to unzip the files
    # if single structure, doc folder present and doc folder at first position, file goes into dc_doc, if not, dc_reste
    # if multi structures, the file goes into dc_multi
    clefs = list(dico.keys())
    dc_doc = {}
    dc_reste = {}
    dc_multi = {}
    for clef in clefs:
        lis = []
        doc = []
        reste = []
        multi = []
        for dic in dico[clef]:
            cl = list(dic.keys())[0]
            liste = [re.sub(r"^\/", "", item).split("/")[0] for item in dic[cl]]
            valeurs = list(set(liste))
            if len(valeurs) == 1:
                if valeurs[0] == "doc":
                    doc.append(cl)
                else:
                    reste.append(cl)
            else:
                multi.append(cl)
            if doc:
                dc_doc[clef] = doc
            if reste:
                dc_reste[clef] = reste
            if multi:
                dc_multi[clef] = multi

    #####################################################################################################

    # dc_missing for files that not already in dc_doc, dc_reste and dc_multi
    clefs = list(dico.keys())
    dc_missing = {}
    for clef in clefs:
        lis = []
        list_dir = os.listdir(clef)
        list_dir2 = list(map(lambda a: a.replace(".zip", ""), list_dir))
        for dic in dico[clef]:
            cl = list(dic.keys())[0]
            lis.append(cl)
        mq = list(set(list_dir2).difference(set(lis)))
        if mq:
            dc_missing[clef] = mq

    #####################################################################################################

    # unzip files in dc_reste, only the XML we are interested in, by checking the name list inside the zipped folder
    clefs = list(dc_reste.keys())
    for clef in clefs:
        for item in dc_reste[clef]:
            with zipfile.ZipFile(clef + item + ".zip", 'r') as zip_ref:
                zip_ref.extractall(clef + item)
                fol = glob.glob(f"{clef}{item}/{item}/doc/*.zip")
                for fl in fol:
                    with zipfile.ZipFile(fl, 'r') as zip_ref:
                        lfiles = zip_ref.namelist()
                        lfiles = [re.findall(".+\d+\.xml", fl) for fl in lfiles]
                        lfiles = [it for it in lfiles if it]
                        lfiles = list(map(lambda a: a[0], lfiles))
                        for fil in lfiles:
                            zip_ref.extract(fil, clef + item)
            cont = os.listdir(clef + item)
            for c in cont:
                if os.path.isdir(clef + item + "/" + c):
                    shutil.rmtree(clef + item + "/" + c)
                else:
                    lfiles = [re.findall(".+\d+\.xml", fl) for fl in cont]
                    lfiles = [it for it in lfiles if it]
                    lfiles = list(map(lambda a: a[0], lfiles))
                    if c not in lfiles:
                        if os.path.isfile(c):
                            os.remove(clef + item + "/" + c)
                    if c == "Volumeid":
                        os.remove(clef + item + "/" + c)
                    if c == "index.xml":
                        os.remove(clef + item + "/" + c)
            os.remove(clef + item + ".zip")
            new = os.listdir(clef + item)
            for fichier in new:
                dirpath = os.path.join(clef, item, fichier)
                new_complete.append(dirpath)

                prefix = f"{clef}{item}".replace(DATA_PATH, "")

                dic_pref_fil["dirpath"].append(dirpath)
                dic_pref_fil["prefix"].append(prefix)
                dic_pref_fil["file"].append(fichier)

    #####################################################################################################

    # unzip files in dc_doc, only the XML we are interested in, by checking the name list inside the zipped folder
    clefs = list(dc_doc.keys())
    for clef in clefs:
        for item in dc_doc[clef]:
            with zipfile.ZipFile(clef + item + ".zip", 'r') as zip_ref:
                zip_ref.extractall(clef + item)
                fol = glob.glob(f"{clef}{item}/doc/*.zip")
                for fl in fol:
                    with zipfile.ZipFile(fl, 'r') as zip_ref:
                        lfiles = zip_ref.namelist()
                        lfiles = [re.findall(".+\d+\.xml", fl) for fl in lfiles]
                        lfiles = [it for it in lfiles if it]
                        lfiles = list(map(lambda a: a[0], lfiles))
                        for fil in lfiles:
                            zip_ref.extract(fil, clef + item)
            cont = os.listdir(clef + item)
            for c in cont:
                if os.path.isdir(clef + item + "/" + c):
                    shutil.rmtree(clef + item + "/" + c)
                else:
                    lfiles = [re.findall(".+\d+\.xml", fl) for fl in cont]
                    lfiles = [it for it in lfiles if it]
                    lfiles = list(map(lambda a: a[0], lfiles))
                    if c not in lfiles:
                        if os.path.isfile(c):
                            os.remove(clef + item + "/" + c)
                    if c == "Volumeid":
                        os.remove(clef + item + "/" + c)
                    if c == "index.xml":
                        os.remove(clef + item + "/" + c)
            os.remove(clef + item + ".zip")
            new = os.listdir(clef + item)
            for fichier in new:
                dirpath = os.path.join(clef, item, fichier)
                new_complete.append(dirpath)

                prefix = f"{clef}{item}".replace(DATA_PATH, "")

                dic_pref_fil["dirpath"].append(dirpath)
                dic_pref_fil["prefix"].append(prefix)
                dic_pref_fil["file"].append(fichier)

    #####################################################################################################

    # check which year folders are missing in the dictionary with full paths by year
    paths2 = list(set(paths).difference(set(dico.keys())))
    paths2.sort()

    paths3 = []

    for path in paths2:
        lzip = glob.glob(f"{path}*.zip")
        if len(lzip) > 0:
            paths3.append(path)

    paths2 = paths3

    # unzip the files inside these folders
    for path in paths2:
        dos = glob.glob(f"{path}*.zip")
        for item in dos:
            with zipfile.ZipFile(item, 'r') as zip_ref:
                lfiles = zip_ref.namelist()
                lfiles = [re.findall(".+\d+\.xml", it) for it in lfiles]
                lfiles = [it for it in lfiles if it]
                lfiles = list(map(lambda a: a[0], lfiles))
                for fil in lfiles:
                    zip_ref.extract(fil, path)
                    folder = item.replace(".zip", "")
                    shutil.move(path + fil, folder)
                    shutil.rmtree(f"{folder}/doc/")

                    dirpath = os.path.join(folder, fil)
                    new_complete.append(dirpath)

                    prefix = f"{folder}".replace(DATA_PATH, "")

                    dic_pref_fil["dirpath"].append(dirpath)
                    dic_pref_fil["prefix"].append(prefix)
                    dic_pref_fil["file"].append(fil)

            os.remove(item)

    #####################################################################################################

    # unzip the files inside weekly folders and put the weekly folders inside a year folder
    dossiers_zip = glob.glob(f"*.zip")

    def annee(file):
        nom = file.replace(".zip", "")
        nom = nom.split("_")
        annee = nom[len(nom) - 2]

        return annee

    for dos in dossiers_zip:
        an = annee(dos)
        nom = dos.replace(".zip", "")
        if an in paths:
            ldos = os.listdir(PATH + an + "/")
            if nom in ldos:
                dossiers_zip.remove(dos)

    for file in dossiers_zip:
        nom = file.replace(".zip", "")
        nom = nom.split("_")
        annee = nom[len(nom) - 2]
        with zipfile.ZipFile(file, 'r') as zip_ref:
            lfiles = zip_ref.namelist()
            lfiles = [re.findall(".+\d+\.xml", item) for item in lfiles]
            lfiles = [item for item in lfiles if item]
            lfiles = list(map(lambda a: a[0], lfiles))
            for fil in lfiles:
                zip_ref.extract(fil)
                folder = file.replace(".zip", "")
                shutil.move(fil, folder)
                shutil.rmtree(f"{folder}/doc/")
        os.remove(file)
        fannee = PATH + annee
        if not os.path.isdir(fannee):
            os.makedirs(fannee)
        shutil.move(folder, fannee)
        new = os.listdir(fannee + "/" + folder)
        for fichier in new:
            dirpath = os.path.join(fannee, folder, fichier)
            new_complete.append(dirpath)

            prefix = f"{fannee}/{folder}".replace(DATA_PATH, "")

            dic_pref_fil["dirpath"].append(dirpath)
            dic_pref_fil["prefix"].append(prefix)
            dic_pref_fil["file"].append(fichier)

    #####################################################################################################

    # unzip the files in dc_multi
    clefs = list(dc_multi.keys())
    for clef in clefs:
        for item in dc_multi[clef]:
            with zipfile.ZipFile(clef + item + ".zip", 'r') as zip_ref:
                zip_ref.extractall(clef + item)
                fol = glob.glob(f"{clef}{item}/*.zip")
                if fol:
                    for fl in fol:
                        with zipfile.ZipFile(fl, 'r') as zip_ref:
                            lfiles = zip_ref.namelist()
                            lfiles = [re.findall(".+\d+\.xml", fl) for fl in lfiles]
                            lfiles = [it for it in lfiles if it]
                            lfiles = list(map(lambda a: a[0], lfiles))
                            for fil in lfiles:
                                zip_ref.extract(fil, clef + item)
                        os.remove(fl)
            if os.path.isdir(f"{clef}{item}/doc/"):
                fol = glob.glob(f"{clef}{item}/doc/*.zip")
                for fl in fol:
                    with zipfile.ZipFile(fl, 'r') as zip_ref:
                        lfiles = zip_ref.namelist()
                        lfiles = [re.findall(".+\d+\.xml", fl) for fl in lfiles]
                        lfiles = [it for it in lfiles if it]
                        lfiles = list(map(lambda a: a[0], lfiles))
                        for fil in lfiles:
                            zip_ref.extract(fil, clef + item)
            cont = os.listdir(clef + item)
            for c in cont:
                if os.path.isdir(clef + item + "/" + c):
                    shutil.rmtree(clef + item + "/" + c)
                else:
                    lfiles = [re.findall(".+\d+\.xml", fl) for fl in cont]
                    lfiles = [it for it in lfiles if it]
                    lfiles = list(map(lambda a: a[0], lfiles))
                    if c not in lfiles:
                        if os.path.isfile(c):
                            os.remove(clef + item + "/" + c)
                    if c == "Volumeid":
                        os.remove(clef + item + "/" + c)
                    if c == "index.xml":
                        os.remove(clef + item + "/" + c)
            os.remove(clef + item + ".zip")
            new = os.listdir(clef + item)
            for fichier in new:
                dirpath = os.path.join(clef, item, fichier)
                new_complete.append(dirpath)

                prefix = f"{clef}{item}".replace(DATA_PATH, "")

                dic_pref_fil["dirpath"].append(dirpath)
                dic_pref_fil["prefix"].append(prefix)
                dic_pref_fil["file"].append(fichier)

    #####################################################################################################

    # unzip files in dc_missing
    clefs = list(dc_missing.keys())
    for clef in clefs:
        dos = glob.glob(f"{clef}*.zip")
        for item in dos:
            with zipfile.ZipFile(item, 'r') as zip_ref:
                lfiles = zip_ref.namelist()
                lfiles = [re.findall(".+\d+\.xml", it) for it in lfiles]
                lfiles = [it for it in lfiles if it]
                lfiles = list(map(lambda a: a[0], lfiles))
                for fil in lfiles:
                    zip_ref.extract(fil, clef)
                    folder = item.replace(".zip", "")
                    if not os.path.isdir(folder):
                        os.makedirs(folder)
                    shutil.move(clef + fil, folder)

                    dirpath = os.path.join(folder, fil)
                    new_complete.append(dirpath)

                    prefix = folder.replace(DATA_PATH, "")

                    dic_pref_fil["dirpath"].append(dirpath)
                    dic_pref_fil["prefix"].append(prefix)
                    dic_pref_fil["file"].append(fil)

                    if os.path.isdir(f"{folder}/doc/"):
                        shutil.rmtree(f"{folder}/doc/")
                os.remove(item)

    #####################################################################################################
    df_pref_fil = pd.DataFrame(data=dic_pref_fil)
    df_pref_fil.loc[:, "fullpath"] = df_pref_fil.loc[:, "prefix"] + "/" + df_pref_fil.loc[:, "file"]

    dirfile = {"dirpath": [], "prefix": [], "file": []}

    session = boto3.Session(region_name='gra', aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"))
    conn = session.client("s3", aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                          aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                          endpoint_url=os.getenv("ENDPOINT_URL"))
    print("Connexion AWS S3", flush=True)

    paginator = conn.get_paginator('list_objects')
    for i in range(2010, 2024):
        operation_parameters = {'Bucket': 'inpi-xmls',
                                'Prefix': f'{i}'}
        print("Start paginator AWS S3", flush=True)
        page_iterator = paginator.paginate(**operation_parameters)

        for page in page_iterator:
            for item in page["Contents"]:
                flpath = item["Key"]
                if flpath in list(df_pref_fil["fullpath"]):
                    file = item["Key"].split("/")[-1]
                    dirp = df_pref_fil.loc[df_pref_fil["fullpath" == flpath], "dirpath"].items()
                    dirfile["dirpath"].append(dirp)
                    pref = df_pref_fil.loc[df_pref_fil["fullpath" == flpath], "prefix"].items()
                    dirfile["prefix"].append(pref)
                    dirfile["file"].append(file)
        print("End paginator AWS S3", flush=True)

    paths_aws = pd.DataFrame(data=dirfile)

    dic_path = subset_df(paths_aws)

    res_futures(dic_path, req_xml_aws)

    #####################################################################################################

    # load files into INPI db
    client = MongoClient(host=os.getenv("MONGO_URI"), connect=True, connectTimeoutMS=360000)
    print(client.server_info(), flush=True)

    db = client['inpi']
    for item in db.list_collection_names():
        db[item].drop()
    new_complete.sort()
    new = [item for item in new_complete if "NEW" in item]
    new.sort()
    df_new = pd.DataFrame(data={"file": new})
    print("Début du chargement de new.", flush=True)
    dict_new = subset_df(df_new)
    res_futures(dict_new, mongo_fill)
    print("Début du chargement de new.", flush=True)

    amd = [item for item in new_complete if "NEW" not in item]
    amd.sort()

    dirfile = {"fullpath": [], "annee_semaine": []}
    for item in amd:
        ppath = item.replace(DATA_PATH, "").split("/")
        dirfile["fullpath"].append(item)
        del ppath[2]
        anse = "_".join(ppath)
        dirfile["annee_semaine"].append(anse)

    df_files = pd.DataFrame(data=dirfile)
    df_files = df_files.sort_values("annee_semaine")
    list_anse = list(set(df_files["annee_semaine"]))
    list_anse.sort()
    for annee_semaine in list_anse:
        liste_annee_semaine = list_anse.split("_")
        print(f"Début de la semaine {liste_annee_semaine[1]} de l'année {liste_annee_semaine[0]}.", flush=True)
        tmp = df_files.loc[df_files["annee_semaine"]==annee_semaine]
        if len(tmp["fullpath"]) > 0:
            tmp = tmp.rename(columns={"fullpath": "file"})
            dict_amd = subset_df(tmp)
            res_futures(dict_amd, mongo_fill)
        print(f"Fin de la semaine {liste_annee_semaine[1]} de l'année {liste_annee_semaine[0]}.", flush=True)
