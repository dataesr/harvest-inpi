import os
import re

from bs4 import BeautifulSoup
from datetime import datetime
from pymongo import MongoClient
import re
from deepdiff import DeepDiff
import pandas as pd
import random

random.seed(1)

DATA_PATH = "/run/media/julia/DATA/INPI/"


def date_pub_ref(lmf, ptdoc):
    global annee
    global semaine
    global date_pdc

    if "date-produced" in ptdoc.attrs.keys():
        date_pdc = datetime.strptime(ptdoc["date-produced"], "%Y%m%d").date().isoformat()
    else:
        for item in lmf:
            if "FR_FR" in item:
                an = re.findall(r"\d{4}", item)
                if len(an) == 1:
                    annee = an[0]
                sem = re.findall(r"\d{2}$", item)
                if len(sem) == 1:
                    semaine = sem[0]

                if annee:
                    if semaine:
                        date_pdc = datetime.strptime(annee + "-W" + semaine + "-1", "%G-W%V-%u").date().isoformat()
                    else:
                        date_pdc = datetime.strptime(annee, "%Y").date().isoformat()

            elif "FR_FR" not in item:
                an = re.findall(r"\d{4}", item)
                today = datetime.today()
                year = today.year + 1
                pannee = [str(ap) for ap in range(2010, year)]
                if len(an) == 1:
                    if an[0] in pannee:
                        annee = an[0]
                        date_pdc = datetime.strptime(annee, "%Y").date().isoformat()
            else:
                if "date_pdc" not in globals():
                    date_pdc = ""

    return date_pdc


def doc_nb(lmf, ptdoc):
    global pb_n
    if "doc-number" in ptdoc.attrs.keys():
        pb_n = ptdoc["doc-number"]
    else:
        for item in lmf:
            if ".xml" in item:
                pb_n = int(item.replace(".xml", ""))

    return pb_n


def stat_pub(lmf, ptdoc):
    global stts
    if "status" in ptdoc.attrs.keys():
        if ptdoc["status"] in ["PUBDEM"]:
            stts = "NEW"
        elif ptdoc["status"] in ["INSCRI", "PUBRRP"]:
            stts = "AMENDED"
        else:
            stts = ptdoc["status"]
    else:
        for item in lmf:
            if "FR_FR" in item:
                item2 = item.replace("FR_FR", "")
                item2 = item2.replace("ST36", "")
                if item2 == "NEW":
                    stts = "NEW"
                elif item2 == "AMD":
                    stts = "AMENDED"
                else:
                    stts = ""

    return stts


def person_ref(bs, pb_n, ptdoc):
    people = ["applicant", "inventor", "agent", "fr-owner", "licensee"]
    liste = []
    for person in people:
        apps = bs.find_all(person)
        if apps:
            lg_apps = len(apps)
            rg = 1
            for item in apps:
                dic_app = {"publication-number": pb_n,
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
                           "type-party": person,
                           "data-format-person": "",
                           "prefix": "",
                           "first-name": "",
                           "middle-name": "",
                           "last-name": "",
                           "orgname": "",
                           "name-complete": "",
                           "suffix": "",
                           "siren": "",
                           "role": "",
                           "department": "",
                           "synonym": "",
                           "designation": "",
                           "application-number-fr": ptdoc["id"]}

                clefs_dic = list(dic_app.keys())
                if "sequence" in item.attrs.keys():
                    dic_app["sequence"] = int(item["sequence"])
                else:
                    dic_app["sequence"] = rg
                    rg = rg + 1
                if "data-format" in item.attrs.keys():
                    dic_app["data-format-person"] = item.attrs["data-format"]
                if "designation" in item.attrs.keys():
                    dic_app["designation"] = item.attrs["designation"]
                tags_item = [tag.name for tag in item.find_all()]
                inter = list(set(clefs_dic).intersection(set(tags_item)))
                for clef in inter:
                    dic_app[clef] = item.find(clef).text
                if "iid" in tags_item:
                    dic_app["siren"] = item.find("iid").text

                ch_name = ""
                for name in ["first-name", "middle-name", "last-name", "orgname"]:
                    if name != "":
                        if ch_name == "":
                            ch_name = dic_app[name].lstrip().rstrip()
                        else:
                            ch_name = ch_name + " " + dic_app[name].lstrip().rstrip()
                    else:
                        pass

                ch_name = re.sub(r"\s+", " ", ch_name)

                dic_app["name-complete"] = ch_name

                liste.append(dic_app)

    return liste


def pub_ref(bs, pb_n, ptdoc):
    lste_pbref = []

    pb_ref = bs.find_all("fr-publication-reference")

    for item in pb_ref:
        dic_pnref = {"data-format-publication": "",
                     "country": "",
                     "publication-number": pb_n,
                     "kind": "",
                     "nature": "",
                     "date-publication": "",
                     "fr-bopinum": "",
                     "application-number-fr": ptdoc["id"]}

        clefs_pnref = list(dic_pnref)
        if "data-format" in item.attrs.keys():
            dic_pnref["data-format-publication"] = item.attrs["data-format"]
        tags_item = [tag.name for tag in item.find_all()]
        inter = list(set(clefs_pnref).intersection(set(tags_item)))
        for clef in inter:
            dic_pnref[clef] = item.find(clef).text
        if dic_pnref["nature"] == "":
            if dic_pnref["kind"] == "A1":
                dic_pnref["nature"] = "Demande française"
            elif dic_pnref["kind"] == "A3":
                dic_pnref["nature"] = "Certificat d\'utilité"

        if "date" in tags_item:
            dic_pnref["date-publication"] = datetime.strptime(item.find("date").text,
                                                              "%Y%m%d").date().isoformat()

        lste_pbref.append(dic_pnref)

    return lste_pbref


def status_list(bs, dic_st):
    lste_stt = []

    stt = bs.find_all("fr-status")

    if stt:
        for item in stt:
            clefs_stt = list(dic_st)
            if "lang" in item.attrs.keys():
                dic_st["lang"] = item.attrs["lang"]
            tags_item = [tag.name for tag in item.find_all()]
            inter = list(set(clefs_stt).intersection(set(tags_item)))
            for clef in inter:
                dic_st[clef] = item.find(clef).text
            lste_stt.append(dic_st)
    else:
        lste_stt.append(dic_st)

    return lste_stt


def renewal_list(bs, pb_n, ptdoc):
    anty = ["fr-last-fee-payement", "fr-next-fee-payement", "fr-penalty"]
    liste = []
    for ant in anty:
        ants = bs.find_all(ant)
        if ants:
            lg_ants = len(ants)
            for item in ants:
                dic_ant = {"publication-number": pb_n,
                           "type-payment": ant,
                           "percentile": "",
                           "date-payment": "",
                           "amount": "",
                           "application-number-fr": ptdoc["id"]}

                clefs_dic = list(dic_ant.keys())
                if "percentile" in item.attrs.keys():
                    dic_ant["percentile"] = item["percentile"]
                if "data-format" in item.attrs.keys():
                    dic_ant["data-format-person"] = item.attrs["data-format"]
                if "designation" in item.attrs.keys():
                    dic_ant["designation"] = item.attrs["designation"]
                tags_item = [tag.name for tag in item.find_all()]
                inter = list(set(clefs_dic).intersection(set(tags_item)))
                for clef in inter:
                    dic_ant[clef] = item.find(clef).text
                if "date" in tags_item:
                    dic_ant["date-payment"] = datetime.strptime(item.find("date").text,
                                                                "%Y%m%d").date().isoformat()

                liste.append(dic_ant)

    return liste


def search_list(bs, pb_n, ptdoc):
    srchl = ["fr-date-search-completed", "fr-date-search-supplemental"]
    cor = {"fr-date-search-completed": "Preliminary search report",
           "fr-date-search-supplemental": "Supplementary search report"}
    liste = []
    for srch in srchl:
        srchs = bs.find_all(srch)
        if srchs:
            for item in srchs:
                dic_srch = {"publication-number": pb_n,
                            "type-search": cor[srch],
                            "date-search": "",
                            "fr-bopinum": "",
                            "application-number-fr": ptdoc["id"]}

                clefs_dic = list(dic_srch.keys())
                dic_srch["fr-bopinum"] = item.find("fr-bopinum").text
                if "date" in [tag.name for tag in item.find_all()]:
                    dic_srch["date-search"] = datetime.strptime(item.find("date").text,
                                                                "%Y%m%d").date().isoformat()

                liste.append(dic_srch)

    return liste


def errata_list(bs, dic_er):
    lste_err = []

    err = bs.find_all("fr-errata")

    if err:
        for item in err:
            clefs_err = list(dic_er)
            tags_item = [tag.name for tag in item.find_all()]
            inter = list(set(clefs_err).intersection(set(tags_item)))
            for clef in inter:
                dic_er[clef] = item.find(clef).text
            if "date" in tags_item:
                dic_er["date-errata"] = datetime.strptime(item.find("date").text,
                                                          "%Y%m%d").date().isoformat()
            part = ""
            if "1" in dic_er["text"]:
                part = "1"
            else:
                if "2" in dic_er["text"]:
                    part = "2"
            dic_er["part"] = part
            lste_err.append(dic_er)
    else:
        lste_err.append(dic_er)

    return lste_err


def amended_list(bs, dic_am):
    lste_am = []

    am = bs.find_all("fr-amended-claim")

    if am:
        for item in am:
            dic_am["claims"] = item.text
            lste_am.append(dic_am)
    else:
        lste_am.append(dic_am)

    return lste_am


def inscr_list(bs, dic_in):
    lste_in = []

    ins = bs.find_all("fr-inscription")

    if ins:
        for item in ins:
            clefs_in = list(dic_in)
            tags_item = [tag.name for tag in item.find_all()]
            inter = list(set(clefs_in).intersection(set(tags_item)))
            for clef in inter:
                dic_in[clef] = item.find(clef).text
            if "date" in tags_item:
                dic_in["date-inscription"] = datetime.strptime(item.find("date").text,
                                                               "%Y%m%d").date().isoformat()
            if "fr-code-inscription" in tags_item:
                dic_in["code-inscription"] = item.find("fr-code-inscription").text

            if "fr-nature-inscription" in tags_item:
                dic_in["nature-inscription"] = item.find("fr-nature-inscription").text
        lste_in.append(dic_in)
    else:
        lste_in.append(dic_in)

    return lste_in


def app_ref(bs, pb_n, ptdoc):
    lste_appref = []

    ap_ref = bs.find_all("fr-application-reference")

    for item in ap_ref:
        dic_apref = {"data-format-application": "",
                     "doc-number": "",
                     "appl-type": "",
                     "country": "",
                     "date-application": "",
                     "application-number-fr": ptdoc["id"],
                     "publication-number": pb_n}

        clefs_pnref = list(dic_apref)
        if "data-format" in item.attrs.keys():
            dic_apref["data-format-application"] = item.attrs["data-format"]
        tags_item = [tag.name for tag in item.find_all()]
        inter = list(set(clefs_pnref).intersection(set(tags_item)))
        for clef in inter:
            dic_apref[clef] = item.find(clef).text
        if "date" in tags_item:
            dic_apref["date-application"] = datetime.strptime(item.find("date").text,
                                                              "%Y%m%d").date().isoformat()

        lste_appref.append(dic_apref)

    return lste_appref


def cit_list(bs, dic_citref):
    lste_cit = []

    cit_ref = bs.find_all("references-cited")

    if cit_ref:
        for item in cit_ref:
            clefs_citref = list(dic_citref)
            tags_item = [tag.name for tag in item.find_all()]
            inter = list(set(clefs_citref).intersection(set(tags_item)))
            for clef in inter:
                dic_citref[clef] = item.find(clef).text
            for clef in tags_item:
                if clef in ["patcit", "nplcit"]:
                    dic_citref["type-citation"] = clef
            if "text" in tags_item:
                dic_citref["citation"] = item.find("text").text
            if "rel-claims" in tags_item:
                dic_citref["claims"] = item.find("rel-claims").text
            if "date" in tags_item:
                dic_citref["date-doc"] = datetime.strptime(item.find("date").text,
                                                           "%Y%m%d").date().isoformat()

            lste_cit.append(dic_citref)
    else:
        lste_cit.append(dic_citref)

    return lste_cit


def prio_list(bs, pn_b, ptdoc):
    lste_prio = []

    prio_ref = bs.find_all("fr-priority-claim")

    if prio_ref:
        for item in prio_ref:
            dic_prio = {"sequence": "",
                        "country": "",
                        "kind": "",
                        "priority-number": "",
                        "date-priority": "",
                        "application-number-fr": ptdoc["id"],
                        "publication-number": pn_b}
            clefs_prioref = list(dic_prio)
            tags_item = [tag.name for tag in item.find_all()]
            inter = list(set(clefs_prioref).intersection(set(tags_item)))
            if "sequence" in item.attrs.keys():
                dic_prio["sequence"] = item["sequence"]
            if "kind" in item.attrs.keys():
                dic_prio["kind"] = item["kind"]
            for clef in inter:
                dic_prio[clef] = item.find(clef).text
            if "doc-number" in tags_item:
                dic_prio["priority-number"] = item.find("doc-number").text
            if "date" in tags_item:
                dic_prio["date-priority"] = datetime.strptime(item.find("date").text,
                                                              "%Y%m%d").date().isoformat()

            lste_prio.append(dic_prio)

    return lste_prio


def redoc_list(bs, pb_n, ptdoc):
    rdc_ref = bs.find_all("related-documents")
    lste_rdc = []

    if rdc_ref:
        for item in rdc_ref:
            dc_rdc = {"type-related-doc": "",
                      "country": "",
                      "doc-number": "",
                      "date-document": "",
                      "application-number-fr": ptdoc["id"],
                      "publication-number": pb_n}

            clefs_rdc = list(dc_rdc)
            tags_item = [tag.name for tag in item.find_all()]
            inter = list(set(clefs_rdc).intersection(set(tags_item)))
            for clef in inter:
                dc_rdc[clef] = item.find(clef).text
            if "division" and "parent-doc" in tags_item:
                dc_rdc["type-related-doc"] = "division de"
            elif "division" and "child-doc" in tags_item:
                dc_rdc["type-related-doc"] = "a pour division"
            elif "name" in tags_item:
                dc_rdc["type-related-doc"] = "CCP rattaché"
            elif "utility-model-basis" and "parent-doc" in tags_item:
                dc_rdc["type-related-doc"] = "transformation volontaire du brevet en certificat d\'utilité"
            elif "date" in tags_item:
                dc_rdc["date-document"] = datetime.strptime(item.find("date").text,
                                                              "%Y%m%d").date().isoformat()
            lste_rdc.append(dc_rdc)

    return lste_rdc


# def read_file(file):
#     with open(file, "r") as f:
#         data = f.read()
#
#     elem_file = file.split("/")
#
#     if len(data) > 0:
#
#         bs_data = BeautifulSoup(data, "xml")
#
#         pn = bs_data.find("fr-patent-document")
#
#         date_produced = date_pub_ref(elem_file, pn)
#
#         pub_n = doc_nb(elem_file, pn)
#
#         stats = stat_pub(elem_file, pn)
#
#         dic_pn = {"lang": pn["lang"],
#                   "application-number-fr": pn["id"],
#                   "country": pn["country"],
#                   "date-produced": date_produced,
#                   "publication-number": pub_n,
#                   "status": stats}
#
#         lapp = person_ref(bs_data, pub_n, pn)
#
#         lste_pbref = pub_ref(bs_data, pub_n, pn)
#
#         lste_apref = app_ref(bs_data, pub_n, pn)
#
#         ex = bs_data.find("fr-extension")
#
#         dic_ext = {"publication-number": pub_n,
#                    "territory": "",
#                    "application-number-fr": pn["id"]}
#
#         if ex:
#             if ex.find("fr-extension-territory"):
#                 dic_ext["territory"] =  ex.find("fr-extension-territory").text
#
#         tit = bs_data.find("invention-title")
#         abst = bs_data.find("abstract")
#
#         dic_ta = {"publication-number": pub_n,
#                   "title": "",
#                   "abstract": "",
#                   "application-number-fr": pn["id"]}
#
#         if tit:
#             dic_ta["title"] =  tit.text.lstrip().rstrip()
#         if abst:
#             dic_ta["abstract"] = abst.text.lstrip().rstrip()
#
#         ptlife = bs_data.find("fr-patent-life")
#
#         dic_grt = {"publication-number": pub_n,
#                    "country": "",
#                    "kind": "",
#                    "date-grant": "",
#                    "fr-bopinum": "",
#                    "application-number-fr": pn["id"]}
#
#         dic_ref = {"publication-number": pub_n,
#                    "date-refusal": "",
#                    "application-number-fr": pn["id"]}
#
#         dic_wd = {"publication-number": pub_n,
#                    "date-withdrawal": "",
#                    "application-number-fr": pn["id"]}
#
#         if ptlife:
#             grt = ptlife.find("fr-date-granted")
#
#             clefs_grt = list(dic_grt)
#             if grt:
#                 tags_item = [tag.name for tag in grt.find_all()]
#                 inter = list(set(clefs_grt).intersection(set(tags_item)))
#                 for clef in inter:
#                     dic_grt[clef] = grt.find(clef).text
#                 if "date" in tags_item:
#                     dic_grt["date-grant"] = datetime.strptime(grt.find("date").text,
#                                                                       "%Y%m%d").date().isoformat()
#
#             ref = ptlife.find("fr-date-application-refused")
#
#             if ref:
#                 dic_ref["date-refusal"] = datetime.strptime(ref.find("date").text,
#                                                                       "%Y%m%d").date().isoformat()
#
#             wd = ptlife.find("fr-date-application-withdrawn")
#
#             if wd:
#                 dic_wd["date-withdrawal"] = datetime.strptime(wd.find("date").text,
#                                                             "%Y%m%d").date().isoformat()
#
#
#     else:
#         dic_pn = {}
#         lapp = []
#         lste_pbref = []
#         lste_apref = []
#         dic_ext = {}
#         dic_ta = {}
#         dic_grt = {}
#         dic_ref = {}
#         dic_wd = {}
#
#
#     return dic_pn, lapp, lste_pbref, lste_apref, dic_ext, dic_ta, dic_grt, dic_ref, dic_wd


def update_db():
    os.chdir(DATA_PATH)

    list_dir = os.listdir(DATA_PATH)
    list_dir.sort()

    dico = {}
    dirfile = {"fullpath": []}
    for dir in list_dir:
        for dirpath, dirs, files in os.walk(f"/run/media/julia/DATA/INPI/{dir}/", topdown=True):
            if dirpath != f"/run/media/julia/DATA/INPI/{dir}/":
                nb_files = round(len(files) * 0.001)
                sp_files = random.sample(files, nb_files)
                dico[dir] = sp_files
                for item in sp_files:
                    if item not in ["index.xml", "Volumeid"]:
                        flpath = dirpath + "/" + item
                        dirfile["fullpath"].append(flpath)

    df_files = pd.DataFrame(data=dirfile)
    df_files["file"] = df_files["fullpath"].str.split("/")
    df_files["pn"] = df_files["file"].apply(lambda a: [x.replace(".xml", "") for x in a if ".xml" in x][0])

    # selection = ["/run/media/julia/DATA/INPI/2019/FR_FRNEWST36_2019_41/2661401.xml",
    #              "/run/media/julia/DATA/INPI/2019/FR_FRAMDST36_2019_42/2661401.xml",
    #              "/run/media/julia/DATA/INPI/2019/FR_FRAMDST36_2019_48/2661401.xml",
    #              "/run/media/julia/DATA/INPI/2021/FR_FRAMDST36_2021_47/2661401.xml",
    #              "/run/media/julia/DATA/INPI/2019/FR_FRNEWST36_2019_41/2685948.xml",
    #              "/run/media/julia/DATA/INPI/2019/FR_FRAMDST36_2019_42/2685948.xml",
    #              "/run/media/julia/DATA/INPI/2019/FR_FRAMDST36_2019_48/2685948.xml",
    #              "/run/media/julia/DATA/INPI/2021/FR_FRAMDST36_2021_23/2685948.xml",
    #              "/run/media/julia/DATA/INPI/2019/FR_FRNEWST36_2019_41/2688327.xml",
    #              "/run/media/julia/DATA/INPI/2019/FR_FRAMDST36_2019_42/2688327.xml",
    #              "/run/media/julia/DATA/INPI/2019/FR_FRAMDST36_2019_48/2688327.xml",
    #              "/run/media/julia/DATA/INPI/2010/FR_FRAMDST36_2010_47/2690701.xml",
    #              "/run/media/julia/DATA/INPI/2020/FR_FRNEWST36_2020_38/2690701.xml",
    #              "/run/media/julia/DATA/INPI/2020/FR_FRAMDST36_2020_38/2690701.xml",
    #              "/run/media/julia/DATA/INPI/2019/FR_FRNEWST36_2019_41/2755307.xml",
    #              "/run/media/julia/DATA/INPI/2019/FR_FRAMDST36_2019_42/2755307.xml",
    #              "/run/media/julia/DATA/INPI/2019/FR_FRAMDST36_2019_48/2755307.xml",
    #              "/run/media/julia/DATA/INPI/2019/FR_FRNEWST36_2019_41/2769229.xml",
    #              "/run/media/julia/DATA/INPI/2019/FR_FRAMDST36_2019_42/2769229.xml",
    #              "/run/media/julia/DATA/INPI/2019/FR_FRAMDST36_2019_48/2769229.xml",
    #              "/run/media/julia/DATA/INPI/2021/FR_FRAMDST36_2021_19/2769229.xml",
    #              "/run/media/julia/DATA/INPI/2022/FR_FRAMDST36_2022_37/2769229.xml",
    #              "/run/media/julia/DATA/INPI/2022/FR_FRAMDST36_2022_45/2769229.xml",
    #              "/run/media/julia/DATA/INPI/2019/FR_FRNEWST36_2019_41/2775579.xml",
    #              "/run/media/julia/DATA/INPI/2019/FR_FRAMDST36_2019_42/2775579.xml",
    #              "/run/media/julia/DATA/INPI/2019/FR_FRAMDST36_2019_48/2775579.xml",
    #              "/run/media/julia/DATA/INPI/2019/FR_FRNEWST36_2019_26/2873022.xml",
    #              "/run/media/julia/DATA/INPI/2020/FR_FRAMDST36_2020_25/2873022.xml",
    #              "/run/media/julia/DATA/INPI/2021/FR_FRAMDST36_2021_04/2873022.xml",
    #              "/run/media/julia/DATA/INPI/2022/FR_FRAMDST36_2022_10/2873022.xml",
    #              "/run/media/julia/DATA/INPI/2022/FR_FRAMDST36_2022_14/2873022.xml",
    #              "/run/media/julia/DATA/INPI/2019/FR_FRNEWST36_2019_26/2873023.xml",
    #              "/run/media/julia/DATA/INPI/2020/FR_FRAMDST36_2020_25/2873023.xml",
    #              "/run/media/julia/DATA/INPI/2021/FR_FRAMDST36_2021_04/2873023.xml",
    #              "/run/media/julia/DATA/INPI/2022/FR_FRAMDST36_2022_10/2873023.xml",
    #              "/run/media/julia/DATA/INPI/2022/FR_FRAMDST36_2022_14/2873023.xml",
    #              "/run/media/julia/DATA/INPI/2010/FR_FRAMDST36_2010_15/2873038.xml",
    #              "/run/media/julia/DATA/INPI/2019/FR_FRNEWST36_2019_30/2873038.xml",
    #              "/run/media/julia/DATA/INPI/2020/FR_FRAMDST36_2020_31/2873038.xml",
    #              "/run/media/julia/DATA/INPI/2021/FR_FRAMDST36_2021_04/2873038.xml",
    #              "/run/media/julia/DATA/INPI/2021/FR_FRAMDST36_2021_31/2873038.xml",
    #              "/run/media/julia/DATA/INPI/2022/FR_FRAMDST36_2022_30/2873038.xml"]
    #
    # dirfile2 = {"fullpath": selection}

    client = MongoClient('mongodb://localhost:27017/')

    db = client['bdd_inpi']

    for item in db.list_collection_names():
        db[item].drop()

    publications = db.publications

    personRef = db.personRef

    publicationRef = db.publicationRef

    applicationRef = db.applicationRef

    extensionPublications = db.extensionPublications

    titleAbstract = db.titleAbstract

    grant = db.grant

    refusal = db.refusal

    withdrawal = db.withdrawal

    notificationLapsed = db.notificationLapsed

    status = db.status

    renewal = db.renewal

    errata = db.errata

    inscriptionPublications = db.inscriptionPublications

    searchPublications = db.searchPublications

    amendedClaims = db.amendedClaims

    citationPublications = db.citationPublications

    priorityPublications = db.priorityPublications
    
    relatedDocuments = db.relatedDocuments

    liste_pn = []
    liste_app = []
    liste_pbref = []
    liste_apref = []
    liste_ext = []
    liste_ta = []
    liste_grant = []
    liste_refusal = []
    liste_withdrawal = []
    liste_lapsed = []
    liste_status = []
    liste_renewal = []
    liste_errata = []
    liste_ins = []
    liste_search = []
    liste_amended = []
    liste_citation = []
    liste_priority = []
    liste_redoc = []
    for file in dirfile["fullpath"]:
        print(file)
        with open(file, "r") as f:
            data = f.read()

        elem_file = file.split("/")

        if len(data) > 0:

            bs_data = BeautifulSoup(data, "xml")

            pn = bs_data.find("fr-patent-document")

            date_produced = date_pub_ref(elem_file, pn)

            pub_n = doc_nb(elem_file, pn)

            stats = stat_pub(elem_file, pn)

            dic_pn = {"lang": pn["lang"],
                      "application-number-fr": pn["id"],
                      "country": pn["country"],
                      "date-produced": date_produced,
                      "publication-number": pub_n,
                      "status": stats}

            appl = person_ref(bs_data, pub_n, pn)

            pref = pub_ref(bs_data, pub_n, pn)

            aref = app_ref(bs_data, pub_n, pn)

            ex = bs_data.find("fr-extension")

            ext = {"publication-number": pub_n,
                   "territory": "",
                   "application-number-fr": pn["id"]}

            if ex:
                if ex.find("fr-extension-territory"):
                    ext["territory"] = ex.find("fr-extension-territory").text

            tit = bs_data.find("invention-title")
            abst = bs_data.find("abstract")

            ta = {"publication-number": pub_n,
                  "title": "",
                  "abstract": "",
                  "application-number-fr": pn["id"]}

            if tit:
                ta["title"] = tit.text.lstrip().rstrip()
            if abst:
                ta["abstract"] = abst.text.lstrip().rstrip()

            ptlife = bs_data.find("fr-patent-life")

            gr = {"publication-number": pub_n,
                  "country": "",
                  "kind": "",
                  "date-grant": "",
                  "fr-bopinum": "",
                  "application-number-fr": pn["id"]}

            rf = {"publication-number": pub_n,
                  "date-refusal": "",
                  "application-number-fr": pn["id"]}

            wid = {"publication-number": pub_n,
                   "date-withdrawal": "",
                   "application-number-fr": pn["id"]}

            lap = {"publication-number": pub_n,
                   "date-lapse": "",
                   "fr-bopinum": "",
                   "application-number-fr": pn["id"]}

            if ptlife:
                grt = ptlife.find("fr-date-granted")

                clefs_grt = list(gr)
                if grt:
                    tags_item = [tag.name for tag in grt.find_all()]
                    inter = list(set(clefs_grt).intersection(set(tags_item)))
                    for clef in inter:
                        gr[clef] = grt.find(clef).text
                    if "date" in tags_item:
                        gr["date-grant"] = datetime.strptime(grt.find("date").text,
                                                             "%Y%m%d").date().isoformat()

                ref = ptlife.find("fr-date-application-refused")

                if ref:
                    rf["date-refusal"] = datetime.strptime(ref.find("date").text,
                                                           "%Y%m%d").date().isoformat()

                wd = ptlife.find("fr-date-application-withdrawn")

                if wd:
                    wid["date-withdrawal"] = datetime.strptime(wd.find("date").text,
                                                               "%Y%m%d").date().isoformat()

                lp = ptlife.find("fr-date-notification-lapsed")

                if lp:
                    lap["date-lapse"] = datetime.strptime(lp.find("date").text,
                                                          "%Y%m%d").date().isoformat()
                    lap["fr-bopinum"] = lp.find("fr-bopinum").text

                dic_stt = {"publication-number": pub_n,
                           "lang": "",
                           "fr-nature": "",
                           "application-number-fr": pn["id"]}

                stus = status_list(ptlife, dic_stt)
                rnw = renewal_list(ptlife, pub_n, pn)

                dic_errata = {"publication-number": pub_n,
                              "part": "",
                              "text": "",
                              "date-errata": "",
                              "fr-bopinum": "",
                              "application-number": pn["id"]}

                erra = errata_list(ptlife, dic_errata)

                dic_ins = {"publication-number": pub_n,
                           "registered-number": "",
                           "date-inscription": "",
                           "code-inscription": "",
                           "nature-inscription": "",
                           "fr-bopinum": "",
                           "application-number": pn["id"]}

                ins = inscr_list(ptlife, dic_ins)

                sear = search_list(ptlife, pub_n, pn)

                dic_amended = {"publication-number": pub_n,
                               "claims": "",
                               "application-number": pn["id"]}

                amend = amended_list(ptlife, dic_amended)

                dic_citations = {"type-citation": "",
                                 "citation": "",
                                 "country": "",
                                 "doc-number": "",
                                 "date-doc": "",
                                 "passage": "",
                                 "category": "",
                                 "claims": "",
                                 "application-number-fr": pn["id"],
                                 "publication-number": pub_n}

                cit = cit_list(ptlife, dic_citations)

                prio = prio_list(bs_data, pub_n, pn)

                redoc = redoc_list(bs_data, pub_n, pn)


        else:
            dic_pn = {}
            appl = []
            pref = []
            aref = []
            ext = {}
            ta = {}
            gr = {}
            rf = {}
            wid = {}
            lap = {}
            stus = []
            rnw = []
            erra = []
            ins = []
            sear = []
            amend = []
            cit = []
            prio = []
            redoc = []
            

        if len(dic_pn) > 0:
            liste_pn.append(dic_pn)
            qr = {"publication-number": dic_pn["publication-number"]}
            mydoc = list(publications.find(qr))
            if len(mydoc) == 0:
                pub_id = publications.insert_one(dic_pn).inserted_id
            else:
                for res in mydoc:
                    diff = DeepDiff(res, dic_pn)
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
                                nwval = {"$set": {k: dic_pn[k]}}
                                x = publications.update_many(qr, nwval, upsert=True)

        if len(appl) > 0:
            liste_app.append(appl)
            for app in appl:
                qr = {"publication-number": app["publication-number"],
                      "application-number-fr": app["application-number-fr"],
                      "sequence": app["sequence"],
                      "type-party": app["type-party"],
                      "first-name": app["first-name"],
                      "middle-name": app["middle-name"],
                      "last-name": app["last-name"],
                      "orgname": app["orgname"]}
                mydoc = list(personRef.find(qr))
                if len(mydoc) == 0:
                    app_id = personRef.insert_one(app).inserted_id
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
                                    x = personRef.update_many(qr, nwval, upsert=True)

        if len(pref) > 0:
            liste_pbref.append(pref)
            for pr in pref:
                qr = {"publication-number": pr["publication-number"],
                      "application-number-fr": pr["application-number-fr"],
                      "data-format-publication": pr["data-format-publication"], "kind": pr["kind"],
                      "nature": pr["nature"],
                      "date-publication": pr["date-publication"], "fr-bopinum": pr["fr-bopinum"]}
                pr_id = publicationRef.update_many(qr, {"$set": qr}, upsert=True)

        if len(aref) > 0:
            liste_apref.append(aref)
            for ar in aref:
                qr = {"data-format-application": ar["data-format-application"],
                      "doc-number": ar["doc-number"],
                      "appl-type": ar["appl-type"],
                      "country": ar["country"],
                      "date-application": ar["date-application"],
                      "application-number-fr": ar["application-number-fr"],
                      "publication-number": ar["publication-number"]}
                ar_id = applicationRef.update_many(qr, {"$set": qr}, upsert=True)

        if len(ext) > 0:
            liste_ext.append(ext)
            qr = {"publication-number": ext["publication-number"]}
            mydoc = list(extensionPublications.find(qr))
            if len(mydoc) == 0:
                ex_id = extensionPublications.insert_one(ext).inserted_id
            else:
                for res in mydoc:
                    diff = DeepDiff(res, ext)
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
                                nwval = {"$set": {k: ext[k]}}
                                x = extensionPublications.update_many(qr, nwval, upsert=True)

        if len(ta) > 0:
            liste_ta.append(ta)
            qr = {"publication-number": ta["publication-number"]}
            mydoc = list(titleAbstract.find(qr))
            if len(mydoc) == 0:
                ta_id = titleAbstract.insert_one(ta).inserted_id
            else:
                for res in mydoc:
                    diff = DeepDiff(res, ta)
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
                                nwval = {"$set": {k: ta[k]}}
                                x = titleAbstract.update_many(qr, nwval, upsert=True)

        if len(gr) > 0:
            liste_grant.append(gr)
            qr = {"publication-number": gr["publication-number"]}
            mydoc = list(grant.find(qr))
            if len(mydoc) == 0:
                gr_id = grant.insert_one(gr).inserted_id
            else:
                for res in mydoc:
                    diff = DeepDiff(res, gr)
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
                                nwval = {"$set": {k: gr[k]}}
                                x = grant.update_many(qr, nwval, upsert=True)

        if len(rf) > 0:
            liste_refusal.append(rf)
            qr = {"publication-number": rf["publication-number"]}
            mydoc = list(refusal.find(qr))
            if len(mydoc) == 0:
                rf_id = refusal.insert_one(rf).inserted_id
            else:
                for res in mydoc:
                    diff = DeepDiff(res, rf)
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
                                nwval = {"$set": {k: rf[k]}}
                                x = refusal.update_many(qr, nwval, upsert=True)

        if len(wid) > 0:
            liste_withdrawal.append(wid)
            qr = {"publication-number": wid["publication-number"]}
            mydoc = list(withdrawal.find(qr))
            if len(mydoc) == 0:
                wd_id = withdrawal.insert_one(wid).inserted_id
            else:
                for res in mydoc:
                    diff = DeepDiff(res, wid)
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
                                nwval = {"$set": {k: wid[k]}}
                                x = withdrawal.update_many(qr, nwval, upsert=True)

        if len(lap) > 0:
            liste_lapsed.append(lap)
            qr = {"publication-number": lap["publication-number"]}
            mydoc = list(notificationLapsed.find(qr))
            if len(mydoc) == 0:
                lp_id = notificationLapsed.insert_one(lap).inserted_id
            else:
                for res in mydoc:
                    diff = DeepDiff(res, lap)
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
                                nwval = {"$set": {k: lap[k]}}
                                x = notificationLapsed.update_many(qr, nwval, upsert=True)

        if len(stus) > 0:
            liste_status.append(stus)
            for st in stus:
                qr = {"publication-number": st["publication-number"]}
                mydoc = list(status.find(qr))
                if len(mydoc) == 0:
                    stt_id = status.insert_one(st).inserted_id
                else:
                    for res in mydoc:
                        diff = DeepDiff(res, st)
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
                                    nwval = {"$set": {k: st[k]}}
                                    x = status.update_many(qr, nwval, upsert=True)

        if len(rnw) > 0:
            liste_renewal.append(rnw)
            for rn in rnw:
                qr = {"publication-number": rn["publication-number"],
                      "type-payment": rn["type-payment"],
                      "percentile": rn["percentile"],
                      "date-payment": rn["date-payment"],
                      "amount": rn["amount"]}
                mydoc = list(renewal.find(qr))
                if len(mydoc) == 0:
                    rnw_id = renewal.insert_one(rn).inserted_id
                else:
                    for res in mydoc:
                        diff = DeepDiff(res, rn)
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
                                    nwval = {"$set": {k: rn[k]}}
                                    x = renewal.update_many(qr, nwval, upsert=True)

        if len(erra) > 0:
            liste_errata.append(erra)
            for ert in erra:
                qr = {"publication-number": ert["publication-number"],
                      "part": ert["part"],
                      "text": ert["text"],
                      "date-errata": ert["date-errata"],
                      "fr-bopinum": ert["fr-bopinum"]}
                mydoc = list(errata.find(qr))
                if len(mydoc) == 0:
                    err_id = errata.insert_one(ert).inserted_id
                else:
                    for res in mydoc:
                        diff = DeepDiff(res, ert)
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
                                    nwval = {"$set": {k: ert[k]}}
                                    x = errata.update_many(qr, nwval, upsert=True)

        if len(ins) > 0:
            liste_ins.append(ins)
            for insc in ins:
                qr = {"publication-number": insc["publication-number"],
                      "registered-number": insc["registered-number"],
                      "date-inscription": insc["date-inscription"],
                      "code-inscription": insc["code-inscription"],
                      "nature-inscription": insc["nature-inscription"],
                      "fr-bopinum": insc["fr-bopinum"],
                      "application-number": insc["application-number"]}
                mydoc = list(inscriptionPublications.find(qr))
                if len(mydoc) == 0:
                    ins_id = inscriptionPublications.insert_one(insc).inserted_id
                else:
                    for res in mydoc:
                        diff = DeepDiff(res, insc)
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
                                    nwval = {"$set": {k: insc[k]}}
                                    x = inscriptionPublications.update_many(qr, nwval, upsert=True)

        if len(sear) > 0:
            liste_search.append(sear)
            for ser in sear:
                qr = {"publication-number": ser["publication-number"],
                      "type-search": ser["type-search"]}
                mydoc = list(searchPublications.find(qr))
                if len(mydoc) == 0:
                    ser_id = searchPublications.insert_one(ser).inserted_id
                else:
                    for res in mydoc:
                        diff = DeepDiff(res, ser)
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
                                    nwval = {"$set": {k: ser[k]}}
                                    x = searchPublications.update_many(qr, nwval, upsert=True)

        if len(amend) > 0:
            liste_amended.append(amend)
            for ame in amend:
                qr = {"publication-number": ame["publication-number"],
                      "claims": ame["claims"]}
                mydoc = list(amendedClaims.find(qr))
                if len(mydoc) == 0:
                    ame_id = amendedClaims.insert_one(ame).inserted_id
                else:
                    for res in mydoc:
                        diff = DeepDiff(res, ame)
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
                                    nwval = {"$set": {k: ame[k]}}
                                    x = amendedClaims.update_many(qr, nwval, upsert=True)

        if len(cit) > 0:
            liste_citation.append(cit)
            for ct in cit:
                qr = {"publication-number": ct["publication-number"],
                      "application-number-fr": ct["application-number-fr"],
                      "type-citation": ct["type-citation"],
                      "citation": ct["citation"],
                      "country": ct["country"],
                      "doc-number": ct["doc-number"],
                      "date-doc": ct["date-doc"],
                      "passage": ct["passage"],
                      "category": ct["category"],
                      "claims": ct["claims"]
                      }
                mydoc = list(citationPublications.find(qr))
                if len(mydoc) == 0:
                    cit_id = citationPublications.insert_one(ct).inserted_id
                else:
                    for res in mydoc:
                        diff = DeepDiff(res, ct)
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
                                    nwval = {"$set": {k: ct[k]}}
                                    x = citationPublications.update_many(qr, nwval, upsert=True)

        if len(prio) > 0:
            liste_priority.append(prio)
            for pri in prio:
                qr = {"publication-number": pri["publication-number"],
                      "application-number-fr": pri["application-number-fr"],
                      "sequence": pri["sequence"],
                      "country": pri["country"],
                      "kind": pri["kind"],
                      "priority-number": pri["priority-number"],
                      "date-priority": pri["date-priority"]
                      }
                mydoc = list(priorityPublications.find(qr))
                if len(mydoc) == 0:
                    prio_id = priorityPublications.insert_one(pri).inserted_id
                else:
                    for res in mydoc:
                        diff = DeepDiff(res, pri)
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
                                    nwval = {"$set": {k: pri[k]}}
                                    x = priorityPublications.update_many(qr, nwval, upsert=True)

        if len(redoc) > 0:
            liste_redoc.append(redoc)
            for rdc in redoc:
                qr = {"type-related-doc": rdc["type-related-doc"],
                      "country": rdc["country"],
                      "doc-number": rdc["doc-number"],
                      "date-document": rdc["date-document"],
                      "application-number-fr": rdc["application-number-fr"],
                      "publication-number": rdc["publication-number"]}
                mydoc = list(relatedDocuments.find(qr))
                if len(mydoc) == 0:
                    rdc_id = relatedDocuments.insert_one(rdc).inserted_id
                else:
                    for res in mydoc:
                        diff = DeepDiff(res, rdc)
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
                                    nwval = {"$set": {k: rdc[k]}}
                                    x = relatedDocuments.update_many(qr, nwval, upsert=True)

    liste_pub = []
    for document in publications.find():
        liste_pub.append(document)
        # print(document)

    df_pub = pd.DataFrame(liste_pub)

    liste_applicants = []
    for document in personRef.find():
        liste_applicants.append(document)
        # print(document)

    df_app = pd.DataFrame(liste_applicants)

    liste_pubref = []
    for document in publicationRef.find():
        liste_pubref.append(document)
        # print(document)

    df_pubref = pd.DataFrame(liste_pubref)

    liste_appref = []
    for document in applicationRef.find():
        liste_appref.append(document)
        # print(document)

    df_appref = pd.DataFrame(liste_appref)

    liste_ext = []
    for document in extensionPublications.find():
        liste_ext.append(document)
        # print(document)

    df_ext = pd.DataFrame(liste_ext)

    liste_ta = []
    for document in titleAbstract.find():
        liste_ta.append(document)
        # print(document)

    df_ta = pd.DataFrame(liste_ta)

    liste_gr = []
    for document in grant.find():
        liste_gr.append(document)
        # print(document)

    df_gr = pd.DataFrame(liste_gr)

    liste_rf = []
    for document in refusal.find():
        liste_rf.append(document)
        # print(document)

    df_rf = pd.DataFrame(liste_rf)

    liste_wid = []
    for document in withdrawal.find():
        liste_wid.append(document)
        # print(document)

    df_wid = pd.DataFrame(liste_wid)

    liste_lps = []
    for document in notificationLapsed.find():
        liste_lps.append(document)
        # print(document)

    df_lps = pd.DataFrame(liste_lps)

    liste_sts = []
    for document in status.find():
        liste_sts.append(document)
        # print(document)

    df_status = pd.DataFrame(liste_sts)

    liste_rnw = []
    for document in renewal.find():
        liste_rnw.append(document)
        # print(document)

    df_rnw = pd.DataFrame(liste_rnw)

    liste_err = []
    for document in errata.find():
        liste_err.append(document)
        # print(document)

    df_err = pd.DataFrame(liste_err)

    liste_inscriptions = []
    for document in inscriptionPublications.find():
        liste_inscriptions.append(document)
        # print(document)

    df_ins = pd.DataFrame(liste_inscriptions)

    liste_search = []
    for document in searchPublications.find():
        liste_search.append(document)
        # print(document)

    df_search = pd.DataFrame(liste_search)

    liste_amend = []
    for document in amendedClaims.find():
        liste_amend.append(document)
        # print(document)

    df_amended = pd.DataFrame(liste_amend)

    liste_cit = []
    for document in citationPublications.find():
        liste_cit.append(document)
        # print(document)

    df_citations = pd.DataFrame(liste_cit)

    liste_prio = []
    for document in priorityPublications.find():
        liste_prio.append(document)
        # print(document)

    df_prio = pd.DataFrame(liste_prio)

    liste_rdc = []
    for document in relatedDocuments.find():
        liste_rdc.append(document)
        # print(document)

    df_rdc = pd.DataFrame(liste_rdc)

    client.close()
