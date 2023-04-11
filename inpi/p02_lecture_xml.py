import os

from bs4 import BeautifulSoup
from datetime import datetime
from pymongo import MongoClient
import re
from deepdiff import DeepDiff
import pandas as pd

DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')


def read_file(file):
    with open(file, "r") as f:
        data = f.read()

    elem_file = file.split("/")

    if len(data) > 0:
        global annee
        global semaine
        global date_pn
        bs_data = BeautifulSoup(data, "xml")

        pn = bs_data.find("fr-patent-document")

        if "date-produced" in pn.attrs.keys():
            date_pn = datetime.strptime(pn["date-produced"], "%Y%m%d").date().isoformat()
        else:
            for item in elem_file:
                if "FR_FR" in item:
                    an = re.findall(r"\d{4}", item)
                    if len(an) == 1:
                        annee = an[0]
                    sem = re.findall(r"\d{2}$", item)
                    if len(sem) == 1:
                        semaine = sem[0]

                    if annee:
                        if semaine:
                            date_pn = datetime.strptime(annee + "-W" + semaine + "-1", "%G-W%V-%u").date().isoformat()
                        else:
                            date_pn = datetime.strptime(annee, "%Y").date().isoformat()

                elif "FR_FR" not in item:
                    an = re.findall(r"\d{4}", item)
                    today = datetime.today()
                    year = today.year + 1
                    pannee = [str(ap) for ap in range(2010, year)]
                    if len(an) == 1:
                        if an[0] in pannee:
                            annee = an[0]
                            date_pn = datetime.strptime(annee, "%Y").date().isoformat()
                else:
                    if "date_pn" not in globals():
                        date_pn = ""

        if "doc-number" in pn.attrs.keys():
            pub_n = int(pn["doc-number"])
        else:
            for item in elem_file:
                if ".xml" in item:
                    pub_n = int(item.replace(".xml", ""))

        global stats
        if "status" in pn.attrs.keys():
            if pn["status"] in ["PUBDEM"]:
                stats = "NEW"
            elif pn["status"] in ["INSCRI", "PUBRRP"]:
                stats = "AMENDED"
            else:
                stats = pn["status"]
        else:
            for item in elem_file:
                if "FR_FR" in item:
                    item2 = item.replace("FR_FR", "")
                    item2 = item2.replace("ST36", "")
                    if item2 == "NEW":
                        stats = "NEW"
                    elif item2 == "AMD":
                        stats = "AMENDED"
                    else:
                        stats = ""

        dic_pn = {"lang": pn["lang"],
                  "application-number": pn["id"],
                  "country": pn["country"],
                  "date-produced": date_pn,
                  "publication-number": int(pn["doc-number"]),
                  "status": stats}

        apps = bs_data.find_all("applicant")

        liste = []

        for item in apps:
            dic_app = {"publication-number": pub_n,
                       "address-1": "",
                       "address-2": "",
                       "address-3": "",
                       "mailcode": "",
                       "pobox": "",
                       "room": "",
                       "address-floor": "",
                       "building": "",
                       "street": "",
                       "city": "",
                       "county": "",
                       "state": "",
                       "postcode": "",
                       "country": "",
                       "sequence": "",
                       "type-party": "",
                       "data-format": "",
                       "prefix": "",
                       "first-name": "",
                       "middle-name": "",
                       "last-name": "",
                       "orgname": "",
                       "suffix": "",
                       "siren": "",
                       "role": "",
                       "department": "",
                       "synonym": "",
                       "designation": "",
                       "application-number": pn["id"]}

            clefs_dic = list(dic_app.keys())
            dic_app["sequence"] = int(item["sequence"])
            dic_app["type-party"] = item["app-type"]
            if "data-format" in item.attrs.keys():
                dic_app["data-format"] = item.attrs["data-format"]
            tags_item = [tag.name for tag in item.find_all()]
            inter = list(set(clefs_dic).intersection(set(tags_item)))
            for clef in inter:
                dic_app[clef] = item.find(clef).text
            if "iid" in tags_item:
                dic_app["siren"] = item.find("iid").text

            liste.append(dic_app)
    else:
        dic_pn = {}
        liste = []

    return dic_pn, liste


def update_db():
    os.chdir(DATA_PATH)

    list_dir = os.listdir(DATA_PATH)
    list_dir.sort()

    dico = {}
    dirfile = {"fullpath": []}
    for directory in list_dir:
        for dirpath, dirs, files in os.walk(f"{DATA_PATH}{directory}/", topdown=True):
            if dirpath != f"{DATA_PATH}{directory}/":
                dico[directory] = files
                for item in files:
                    if item not in ["index.xml", "Volumeid"]:
                        flpath = dirpath + "/" + item
                        dirfile["fullpath"].append(flpath)

    df_files = pd.DataFrame(data=dirfile)
    df_files["file"] = df_files["fullpath"].str.split("/")
    df_files["pn"] = df_files["file"].apply(lambda a: [x.replace(".xml", "") for x in a if ".xml" in x][0])

    client = MongoClient('mongodb://localhost:27017/')

    db = client['bdd_inpi']

    publications = db.publications
    category_index = publications.create_index("publication-number")

    applicants = db.applicants

    liste_pn = []
    liste_app = []
    for item in dirfile["fullpath"]:
        pn, appl = read_file(item)
        if len(pn) > 0:
            liste_pn.append(pn)
            qr = {"publication-number": pn["publication-number"]}
            mydoc = list(publications.find(qr))
            if len(mydoc) == 0:
                pub_id = publications.insert_one(pn).inserted_id
            else:
                for res in mydoc:
                    diff = DeepDiff(res, pn)
                    if len(diff) > 0:
                        if "values_changed" in diff.keys():
                            d = diff["values_changed"]
                            ks = list(d.keys())
                            tks = []
                            for k in ks:
                                k = k.replace("root['", "")
                                k = k.replace("']", "")
                                tks.append(k)

                            for k in tks:
                                nwval = {"$set": {k: pn[k]}}
                                x = publications.update_many(qr, nwval, upsert=True)

        if len(appl) > 0:
            liste_app.append(appl)
            for app in appl:
                qr = {"publication-number": app["publication-number"], "application-number": app["application-number"],
                      "sequence": app["sequence"]}
                mydoc = list(applicants.find(qr))
                if len(mydoc) == 0:
                    app_id = applicants.insert_one(app).inserted_id
                else:
                    for res in mydoc:
                        diff = DeepDiff(res, app)
                        if len(diff) > 0:
                            if "values_changed" in diff.keys():
                                d = diff["values_changed"]
                                ks = list(d.keys())
                                tks = []
                                for k in ks:
                                    k = k.replace("root['", "")
                                    k = k.replace("']", "")
                                    tks.append(k)

                                for k in tks:
                                    nwval = {"$set": {k: app[k]}}
                                    x = applicants.update_many(qr, nwval, upsert=True)

    client.close()
