#!/usr/bin/env python
# coding: utf-8

import glob
import os
import zipfile
import re
import shutil
import tarfile
# import boto3
from inpi import p02_lecture_xml as p02
from pymongo import MongoClient


# directory where the files are
DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')


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
                # conn = boto3.client("s3", aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                #                     aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                #                     endpoint_url=os.getenv("ENDPOINT_URL"))

                dirpath = os.path.join(clef, item, fichier)
                new_complete.append(dirpath)

                # prefix = f"{clef}{item}".replace(DATA_PATH, "")

                # try:
                #     # response = conn.upload_file(dirpath, "inpi-xmls", f"{prefix}/{fichier}")
                #     # print(f"{prefix}/{fichier} added in inpi-xmls", flush=True)
                #     # data = conn.get_object(Bucket="inpi-xmls", Key=f"{prefix}/{fichier}").get("Body").read().decode()
                #     with open(dirpath, "r") as f:
                #         data = f.read()
                #     p02.update_db(dirpath, data)
                # except boto3.exceptions.S3UploadFailedError as error:
                #     print(error.response, flush=True)
                #     raise error
                # except:
                #     print("erreur de chargement")

                # conn.close()

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
                # conn = boto3.client("s3", aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                #                     aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                #                     endpoint_url=os.getenv("ENDPOINT_URL"))

                dirpath = os.path.join(clef, item, fichier)
                new_complete.append(dirpath)

                # prefix = f"{clef}{item}".replace(DATA_PATH, "")
                #
                # try:
                #     response = conn.upload_file(dirpath, "inpi-xmls", f"{prefix}/{fichier}")
                #     print(f"{prefix}/{fichier} added in inpi-xmls", flush=True)
                #     data = conn.get_object(Bucket="inpi-xmls", Key=f"{prefix}/{fichier}").get("Body").read().decode()
                #     p02.update_db(f"{prefix}/{fichier}", data)
                # except boto3.exceptions.S3UploadFailedError as error:
                #     print(error.response, flush=True)
                #     raise error
                #
                # conn.close()

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
                    # conn = boto3.client("s3", aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                    #                     aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                    #                     endpoint_url=os.getenv("ENDPOINT_URL"))

                    dirpath = os.path.join(folder, fil)
                    new_complete.append(dirpath)

                    # prefix = f"{folder}".replace(DATA_PATH, "")
                    #
                    # try:
                    #     response = conn.upload_file(dirpath, "inpi-xmls", f"{prefix}/{fil}")
                    #     print(f"{prefix}/{fil} added in inpi-xmls", flush=True)
                    #     data = conn.get_object(Bucket="inpi-xmls", Key=f"{prefix}/{fil}").get(
                    #         "Body").read().decode()
                    #     p02.update_db(f"{prefix}/{fil}", data)
                    # except boto3.exceptions.S3UploadFailedError as error:
                    #     print(error.response, flush=True)
                    #     raise error
                    #
                    # conn.close()
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
            # conn = boto3.client("s3", aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            #                     aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            #                     endpoint_url=os.getenv("ENDPOINT_URL"))

            dirpath = os.path.join(fannee, folder, fichier)
            new_complete.append(dirpath)

            # prefix = f"{fannee}/{folder}".replace(DATA_PATH, "")
            #
            # try:
            #     response = conn.upload_file(dirpath, "inpi-xmls", f"{prefix}/{fichier}")
            #     print(f"{prefix}/{fichier} added in inpi-xmls", flush=True)
            #     data = conn.get_object(Bucket="inpi-xmls", Key=f"{prefix}/{fichier}").get(
            #         "Body").read().decode()
            #     p02.update_db(f"{prefix}/{fichier}", data)
            # except boto3.exceptions.S3UploadFailedError as error:
            #     print(error.response, flush=True)
            #     raise error
            #
            # conn.close()

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
                # conn = boto3.client("s3", aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                #                     aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                #                     endpoint_url=os.getenv("ENDPOINT_URL"))

                dirpath = os.path.join(clef, item, fichier)
                new_complete.append(dirpath)

                # prefix = f"{clef}{item}".replace(DATA_PATH, "")
                #
                # try:
                #     response = conn.upload_file(dirpath, "inpi-xmls", f"{prefix}/{fichier}")
                #     print(f"{prefix}/{fichier} added in inpi-xmls", flush=True)
                #     data = conn.get_object(Bucket="inpi-xmls", Key=f"{prefix}/{fichier}").get(
                #         "Body").read().decode()
                #     p02.update_db(f"{prefix}/{fichier}", data)
                # except boto3.exceptions.S3UploadFailedError as error:
                #     print(error.response, flush=True)
                #     raise error
                #
                # conn.close()

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
                    # conn = boto3.client("s3", aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                    #                     aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                    #                     endpoint_url=os.getenv("ENDPOINT_URL"))

                    dirpath = os.path.join(folder, fil)
                    new_complete.append(dirpath)

                    # prefix = folder.replace(DATA_PATH, "")
                    #
                    # try:
                    #     response = conn.upload_file(dirpath, "inpi-xmls", f"{prefix}/{fil}")
                    #     print(f"{prefix}/{fil} added in inpi-xmls", flush=True)
                    #     data = conn.get_object(Bucket="inpi-xmls", Key=f"{prefix}/{fil}").get(
                    #         "Body").read().decode()
                    #     p02.update_db(f"{prefix}/{fil}", data)
                    # except boto3.exceptions.S3UploadFailedError as error:
                    #     print(error.response, flush=True)
                    #     raise error
                    #
                    # conn.close()
                    if os.path.isdir(f"{folder}/doc/"):
                        shutil.rmtree(f"{folder}/doc/")
                os.remove(item)

    #####################################################################################################

    # load files into INPI db
    client = MongoClient(host=os.getenv("MONGO_URI"), connect=True, connectTimeoutMS=360000)
    print(client.server_info(), flush=True)

    db = client['inpi']
    for item in db.list_collection_names():
        db[item].drop()
    new_complete.sort()
    for file in new_complete:
        with open(file, "r") as f:
            data = f.read()
        p02.update_db(file, data)
