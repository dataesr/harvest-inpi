import os
import random
import re
from datetime import datetime

import pandas as pd
from bs4 import BeautifulSoup
from deepdiff import DeepDiff
from pymongo import MongoClient

random.seed(42)

DATA_PATH = "/run/media/julia/DATA/INPI/"


def check_date(dae: str) -> str:
    if len(dae) == 8:
        mois = dae[4:6]
        jour = dae[6:]
        if int(mois) <= 12:
            if int(jour) <= 31:
                dte = datetime.strptime(dae, "%Y%m%d").date().isoformat()
            else:
                dte = dae
        else:
            dte = dae
    else:
        dte = dae

    return dte


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

    if len(liste) == 0:
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
                   "type-party": "",
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
        liste.append(dic_app)

    df = pd.DataFrame(data=liste).drop_duplicates()

    return df


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
            dae = str(item.find("date").text)
            dic_pnref["date-publication"] = check_date(dae)

        lste_pbref.append(dic_pnref)

    if len(lste_pbref) == 0:
        lste_pbref.append(dic_pnref)

    df = pd.DataFrame(data=lste_pbref).drop_duplicates()

    return df


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
                    dae = item.find("date").text
                    dic_ant["date-payment"] = check_date(dae)

                liste.append(dic_ant)

    if len(liste) == 0:
        dic_ant = {"publication-number": pb_n,
                   "type-payment": "",
                   "percentile": "",
                   "date-payment": "",
                   "amount": "",
                   "application-number-fr": ptdoc["id"]}
        liste.append(dic_ant)

    df = pd.DataFrame(data=liste).drop_duplicates()

    return df


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
                    date_search = item.find("date").text
                    dic_srch["date-search"] = check_date(date_search)

                liste.append(dic_srch)
    if len(liste) == 0:
        dic_srch = {"publication-number": pb_n,
                    "type-search": "",
                    "date-search": "",
                    "fr-bopinum": "",
                    "application-number-fr": ptdoc["id"]}
        liste.append(dic_srch)

    df = pd.DataFrame(data=liste).drop_duplicates()

    return df


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
                dae = item.find("date").text
                dic_er["date-errata"] = check_date(dae)

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

    if len(lste_err) == 0:
        lste_err.append(dic_er)

    df = pd.DataFrame(data=lste_err).drop_duplicates()

    return df


def amended_list(bs, dic_am):
    lste_am = []

    am = bs.find_all("fr-amended-claim")

    if am:
        for item in am:
            dic_am["claim"] = item.text
            lste_am.append(dic_am)
    else:
        lste_am.append(dic_am)

    if len(lste_am) == 0:
        lste_am.append(dic_am)

    df = pd.DataFrame(data=lste_am).drop_duplicates()

    return df


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
                dae = item.find("date").text
                dic_in["date-inscription"] = check_date(dae)

            if "fr-code-inscription" in tags_item:
                dic_in["code-inscription"] = item.find("fr-code-inscription").text

            if "fr-nature-inscription" in tags_item:
                dic_in["nature-inscription"] = item.find("fr-nature-inscription").text
        lste_in.append(dic_in)
    else:
        lste_in.append(dic_in)

    if len(lste_in) == 0:
        lste_in.append(dic_in)

    df = pd.DataFrame(data=lste_in).drop_duplicates()

    return df


def app_ref(bs, pb_n, ptdoc):
    lste_appref = []

    ap_ref = bs.find_all("fr-application-reference")

    if ap_ref:
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
                dae = item.find("date").text
                dic_apref["date-application"] = check_date(dae)

            lste_appref.append(dic_apref)

    else:
        dic_apref = {"data-format-application": "",
                     "doc-number": "",
                     "appl-type": "",
                     "country": "",
                     "date-application": "",
                     "application-number-fr": ptdoc["id"],
                     "publication-number": pb_n}
        lste_appref.append(dic_apref)

    if len(lste_appref) == 0:
        dic_apref = {"data-format-application": "",
                     "doc-number": "",
                     "appl-type": "",
                     "country": "",
                     "date-application": "",
                     "application-number-fr": ptdoc["id"],
                     "publication-number": pb_n}
        lste_appref.append(dic_apref)

    df = pd.DataFrame(data=lste_appref).drop_duplicates()

    return df


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
                dic_citref["claim"] = item.find("rel-claims").text
            if "date" in tags_item:
                dae = item.find("date").text
                dic_citref["date-doc"] = check_date(dae)

            lste_cit.append(dic_citref)
    else:
        lste_cit.append(dic_citref)

    if len(lste_cit) == 0:
        lste_cit.append(dic_citref)

    df = pd.DataFrame(data=lste_cit).drop_duplicates()

    return df


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
                dae = item.find("date").text
                dic_prio["date-priority"] = check_date(dae)

            lste_prio.append(dic_prio)

    else:
        dic_prio = {"sequence": "",
                    "country": "",
                    "kind": "",
                    "priority-number": "",
                    "date-priority": "",
                    "application-number-fr": ptdoc["id"],
                    "publication-number": pn_b}
        lste_prio.append(dic_prio)

    if len(lste_prio) == 0:
        dic_prio = {"sequence": "",
                    "country": "",
                    "kind": "",
                    "priority-number": "",
                    "date-priority": "",
                    "application-number-fr": ptdoc["id"],
                    "publication-number": pn_b}
        lste_prio.append(dic_prio)

    df = pd.DataFrame(data=lste_prio).drop_duplicates()

    return df


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
                dae = item.find("date").text
                dc_rdc["date-document"] = check_date(dae)
            lste_rdc.append(dc_rdc)
    else:
        dc_rdc = {"type-related-doc": "",
                  "country": "",
                  "doc-number": "",
                  "date-document": "",
                  "application-number-fr": ptdoc["id"],
                  "publication-number": pb_n}
        lste_rdc.append(dc_rdc)

    if len(lste_rdc) == 0:
        dc_rdc = {"type-related-doc": "",
                  "country": "",
                  "doc-number": "",
                  "date-document": "",
                  "application-number-fr": ptdoc["id"],
                  "publication-number": pb_n}
        lste_rdc.append(dc_rdc)

    df = pd.DataFrame(data=lste_rdc).drop_duplicates()

    return df


def oldipc_list(bs, pb_n, ptdoc):
    oldipc_ref = bs.find_all("classifications-ipc")
    lste_oipc = []

    if oldipc_ref:
        for item in oldipc_ref:
            dc_oipc = {"edition": "",
                       "main-classification": "",
                       "further-classification-sequence": "",
                       "further-classification": "",
                       "application-number-fr": ptdoc["id"],
                       "publication-number": pb_n}

            clefs_oipc = list(dc_oipc)
            tags_item = [tag.name for tag in item.find_all()]
            inter = list(set(clefs_oipc).intersection(set(tags_item)))
            if "sequence" in item.attrs.keys():
                dc_oipc["further-classification-sequence"] = int(item["sequence"])
            for clef in inter:
                dc_oipc[clef] = item.find(clef).text
            lste_oipc.append(dc_oipc)
    else:
        dc_oipc = {"edition": "",
                   "main-classification": "",
                   "further-classification-sequence": "",
                   "further-classification": "",
                   "application-number-fr": ptdoc["id"],
                   "publication-number": pb_n}
        lste_oipc.append(dc_oipc)

    if len(lste_oipc) == 0:
        dc_oipc = {"edition": "",
                   "main-classification": "",
                   "further-classification-sequence": "",
                   "further-classification": "",
                   "application-number-fr": ptdoc["id"],
                   "publication-number": pb_n}
        lste_oipc.append(dc_oipc)

    df = pd.DataFrame(data=lste_oipc).drop_duplicates()

    return df


def ipc_list(bs, pb_n, ptdoc):
    ipc_ref = bs.find_all("classification-ipcr")
    lste_ipc = []

    if ipc_ref:
        for item in ipc_ref:
            dc_ipc = {"classification": "",
                      "sequence": "",
                      "application-number-fr": ptdoc["id"],
                      "publication-number": pb_n}

            clefs_ipc = list(dc_ipc)
            tags_item = [tag.name for tag in item.find_all()]
            if "text" in tags_item:
                dc_ipc["classification"] = item.find("text").text
            if "sequence" in item.attrs.keys():
                dc_ipc["sequence"] = int(item["sequence"])
            lste_ipc.append(dc_ipc)
    else:
        dc_ipc = {"classification": "",
                  "sequence": "",
                  "application-number-fr": ptdoc["id"],
                  "publication-number": pb_n}
        lste_ipc.append(dc_ipc)

    if len(lste_ipc) == 0:
        dc_ipc = {"classification": "",
                  "sequence": "",
                  "application-number-fr": ptdoc["id"],
                  "publication-number": pb_n}
        lste_ipc.append(dc_ipc)

    df = pd.DataFrame(data=lste_ipc).drop_duplicates()

    return df


def cpc_list(bs, pb_n, ptdoc):
    cpc_ref = bs.find_all("patent-classification")
    lste_cpc = []

    if cpc_ref:
        for item in cpc_ref:
            dc_cpc = {"sequence": "",
                      "scheme": "",
                      "office": "",
                      "date-cpc": "",
                      "symbol": "",
                      "position": "",
                      "value": "",
                      "status": "",
                      "source": "",
                      "date-classification": "",
                      "application-number-fr": ptdoc["id"],
                      "publication-number": pb_n}

            clefs_cpc = list(dc_cpc)
            tags_item = [tag.name for tag in item.find_all()]
            inter = list(set(clefs_cpc).intersection(set(tags_item)))
            scheme_ref = item.find_all("classification-scheme")
            if scheme_ref:
                if len(scheme_ref) == 1:
                    for it in scheme_ref:
                        dc_cpc["scheme"] = it['scheme']
                        dc_cpc["office"] = it['office']
            if "sequence" in item.attrs.keys():
                dc_cpc["sequence"] = int(item["sequence"])
            if "classification-symbol" in tags_item:
                dc_cpc["symbol"] = item.find("classification-symbol").text
            if "symbol-position" in tags_item:
                dc_cpc["position"] = item.find("symbol-position").text
            if "classification-value" in tags_item:
                dc_cpc["value"] = item.find("classification-value").text
            if "classification-status" in tags_item:
                dc_cpc["status"] = item.find("classification-status").text
            if "classification-data-source" in tags_item:
                dc_cpc["source"] = item.find("classification-data-source").text
            if "date" in tags_item:
                dae = item.find("date").text
                dc_cpc["date-cpc"] = check_date(dae)

            if "action-date" in tags_item:
                dae = item.find("action-date").find("date").text
                dc_cpc["date-classification"] = check_date(dae)

            lste_cpc.append(dc_cpc)

    else:
        dc_cpc = {"sequence": "",
                  "scheme": "",
                  "office": "",
                  "date-cpc": "",
                  "symbol": "",
                  "position": "",
                  "value": "",
                  "status": "",
                  "source": "",
                  "date-classification": "",
                  "application-number-fr": ptdoc["id"],
                  "publication-number": pb_n}
        lste_cpc.append(dc_cpc)

    if len(lste_cpc) == 0:
        dc_cpc = {"sequence": "",
                  "scheme": "",
                  "office": "",
                  "date-cpc": "",
                  "symbol": "",
                  "position": "",
                  "value": "",
                  "status": "",
                  "source": "",
                  "date-classification": "",
                  "application-number-fr": ptdoc["id"],
                  "publication-number": pb_n}
        lste_cpc.append(dc_cpc)

    df = pd.DataFrame(data=lste_cpc).drop_duplicates()

    return df


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
                nb_files = round(len(files) * 0.0001)
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

    db = client['inpi']

    for item in db.list_collection_names():
        db[item].drop()

    publication = db.publication

    person = db.person

    publicationRef = db.publicationRef

    application = db.application

    status = db.status

    renewal = db.renewal

    errata = db.errata

    inscription = db.inscription

    search = db.search

    amendedClaim = db.amendedClaim

    citation = db.citation

    priority = db.priority

    relatedDocument = db.relatedDocument

    oldIpc = db.oldIpc

    ipc = db.ipc

    cpc = db.cpc

    liste_pn = []
    liste_app = []
    liste_pbref = []
    liste_apref = []
    liste_status = []
    liste_renewal = []
    liste_errata = []
    liste_ins = []
    liste_search = []
    liste_amended = []
    liste_citation = []
    liste_priority = []
    liste_redoc = []
    liste_oldipc = []
    liste_ipc = []
    liste_cpc = []
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
                      "status": stats,
                      "fr-nature": "",
                      "fr-extension-territory": "",
                      "title": "",
                      "abstract": "",
                      "kind-grant": "",
                      "date-grant": "",
                      "fr-bopinum-grant": "",
                      "date-refusal": "",
                      "date-withdrawal": "",
                      "date-lapsed": "",
                      "fr-bopinum-lapsed": ""
                      }

            ex = bs_data.find("fr-extension")

            if ex:
                if ex.find("fr-extension-territory"):
                    dic_pn["fr-extension-territory"] = ex.find("fr-extension-territory").text

            tit = bs_data.find("invention-title")
            abst = bs_data.find("abstract")
            if tit:
                dic_pn["title"] = tit.text.lstrip().rstrip()
            if abst:
                dic_pn["abstract"] = abst.text.lstrip().rstrip()

            appl = person_ref(bs_data, pub_n, pn)

            pref = pub_ref(bs_data, pub_n, pn)

            aref = app_ref(bs_data, pub_n, pn)

            ptlife = bs_data.find("fr-patent-life")

            if ptlife:
                grt = ptlife.find("fr-date-granted")

                if grt:
                    tags_item = [tag.name for tag in grt.find_all()]
                    if "kind" in tags_item:
                        dic_pn["kind-grant"] = grt.find("kind").text
                    if "fr-bopinum" in tags_item:
                        dic_pn["fr-bopinum-grant"] = grt.find("fr-bopinum").text
                    if "date" in tags_item:
                        dae = grt.find("date").text
                        dic_pn["date-grant"] = check_date(dae)

                ref = ptlife.find("fr-date-application-refused")

                if ref:
                    dae = ref.find("date").text
                    dic_pn["date-refusal"] = check_date(dae)

                wd = ptlife.find("fr-date-application-withdrawn")

                if wd:
                    dae = wd.find("date").text
                    dic_pn["date-withdrawal"] = check_date(dae)

                lp = ptlife.find("fr-date-notification-lapsed")

                if lp:
                    dae = lp.find("date").text
                    dic_pn["date-lapsed"] = check_date(dae)
                    dic_pn["fr-bopinum-lapsed"] = lp.find("fr-bopinum").text

                dic_stt = {"publication-number": pub_n,
                           "lang": "",
                           "fr-nature": "",
                           "application-number-fr": pn["id"]}

                stt = ptlife.find("fr-status")

                if stt:
                    dic_pn["fr-nature"] = stt.find("fr-nature").text

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
                               "claim": "",
                               "application-number": pn["id"]}

                amend = amended_list(ptlife, dic_amended)

                dic_citations = {"type-citation": "",
                                 "citation": "",
                                 "country": "",
                                 "doc-number": "",
                                 "date-doc": "",
                                 "passage": "",
                                 "category": "",
                                 "claim": "",
                                 "application-number-fr": pn["id"],
                                 "publication-number": pub_n}

                cit = cit_list(ptlife, dic_citations)

            else:
                stus = pd.DataFrame(data=[{"publication-number": pub_n,
                                           "lang": "",
                                           "fr-nature": "",
                                           "application-number-fr": pn["id"]}])

                rnw = pd.DataFrame(data=[{"publication-number": pub_n,
                                          "type-payment": "",
                                          "percentile": "",
                                          "date-payment": "",
                                          "amount": "",
                                          "application-number-fr": pn["id"]}])

                erra = pd.DataFrame(data=[{"publication-number": pub_n,
                                           "part": "",
                                           "text": "",
                                           "date-errata": "",
                                           "fr-bopinum": "",
                                           "application-number": pn["id"]}]).drop_duplicates()

                ins = pd.DataFrame(data=[{"publication-number": pub_n,
                                          "registered-number": "",
                                          "date-inscription": "",
                                          "code-inscription": "",
                                          "nature-inscription": "",
                                          "fr-bopinum": "",
                                          "application-number": pn["id"]}]).drop_duplicates()

                sear = pd.DataFrame(data=[{"publication-number": pub_n,
                                           "type-search": "",
                                           "date-search": "",
                                           "fr-bopinum": "",
                                           "application-number-fr": pn["id"]}]).drop_duplicates()

                amend = pd.DataFrame(data=[{"publication-number": pub_n,
                                            "claim": "",
                                            "application-number": pn["id"]}]).drop_duplicates()

                cit = pd.DataFrame(data=[{"type-citation": "",
                                          "citation": "",
                                          "country": "",
                                          "doc-number": "",
                                          "date-doc": "",
                                          "passage": "",
                                          "category": "",
                                          "claim": "",
                                          "application-number-fr": pn["id"],
                                          "publication-number": pub_n}]).drop_duplicates()

            dic_pn = pd.DataFrame(data=[dic_pn]).drop_duplicates()

            prio = prio_list(bs_data, pub_n, pn)

            redoc = redoc_list(bs_data, pub_n, pn)

            oldipc = oldipc_list(bs_data, pub_n, pn)

            ipcs = ipc_list(bs_data, pub_n, pn)

            cpcs = cpc_list(bs_data, pub_n, pn)


        else:
            dic_pn = pd.DataFrame(data=[])
            appl = pd.DataFrame(data=[])
            pref = pd.DataFrame(data=[])
            aref = pd.DataFrame(data=[])
            stus = pd.DataFrame(data=[])
            rnw = pd.DataFrame(data=[])
            erra = pd.DataFrame(data=[])
            ins = pd.DataFrame(data=[])
            sear = pd.DataFrame(data=[])
            amend = pd.DataFrame(data=[])
            cit = pd.DataFrame(data=[])
            prio = pd.DataFrame(data=[])
            redoc = pd.DataFrame(data=[])
            oldipc = pd.DataFrame(data=[])
            ipcs = pd.DataFrame(data=[])
            cpcs = pd.DataFrame(data=[])

        if len(dic_pn) > 0:
            liste_pn.append(dic_pn)
            qr = {"publication-number": dic_pn["publication-number"].item()}
            mydoc = list(publication.find(qr))
            if len(mydoc) == 0:
                pub_id = publication.insert_one(dic_pn.to_dict("records")[0]).inserted_id
            else:
                for res in mydoc:
                    for clef in ["title", "abstract"]:
                        if clef in res.keys():
                            if dic_pn[clef].item() != "":
                                if res[clef] == "":
                                    del res[clef]
                    if res:
                        diff = DeepDiff(res, dic_pn.to_dict("records")[0])
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
                                    nwval = {"$set": {k: dic_pn[k].item()}}
                                    x = publication.update_many(qr, nwval, upsert=True)

        if len(appl) > 0:
            liste_app.append(appl)
            for _, app in appl.iterrows():
                qr = {"publication-number": app["publication-number"],
                      "application-number-fr": app["application-number-fr"],
                      "sequence": app["sequence"],
                      "type-party": app["type-party"],
                      "first-name": app["first-name"],
                      "middle-name": app["middle-name"],
                      "last-name": app["last-name"],
                      "orgname": app["orgname"]}
                mydoc = list(person.find(qr))
                if len(mydoc) == 0:
                    app_id = person.insert_one(app.to_dict()).inserted_id
                else:
                    for res in mydoc:
                        diff = DeepDiff(res, app.to_dict())
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
                                    x = person.update_many(qr, nwval, upsert=True)

        if len(pref) > 0:
            liste_pbref.append(pref)
            for _, pr in pref.iterrows():
                qr = {"publication-number": pr["publication-number"],
                      "application-number-fr": pr["application-number-fr"],
                      "data-format-publication": pr["data-format-publication"],
                      "kind": pr["kind"],
                      "nature": pr["nature"],
                      "date-publication": pr["date-publication"],
                      "fr-bopinum": pr["fr-bopinum"]}
                mydoc = list(publicationRef.find(qr))
                if len(mydoc) == 0:
                    pr_id = publicationRef.insert_one(pr.to_dict()).inserted_id
                else:
                    for res in mydoc:
                        diff = DeepDiff(res, pr.to_dict())
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
                                    nwval = {"$set": {k: pr[k]}}
                                    x = publicationRef.update_many(qr, nwval, upsert=True)

        if len(aref) > 0:
            liste_apref.append(aref)
            for _, ar in aref.iterrows():
                qr = {"data-format-application": ar["data-format-application"],
                      "doc-number": ar["doc-number"],
                      "appl-type": ar["appl-type"],
                      "country": ar["country"],
                      "date-application": ar["date-application"],
                      "application-number-fr": ar["application-number-fr"],
                      "publication-number": ar["publication-number"]}
                mydoc = list(application.find(qr))
                if len(mydoc) == 0:
                    ar_id = application.insert_one(ar.to_dict()).inserted_id
                else:
                    for res in mydoc:
                        diff = DeepDiff(res, ar.to_dict())
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
                                    nwval = {"$set": {k: ar[k]}}
                                    x = application.update_many(qr, nwval, upsert=True)

        if len(stus) > 0:
            liste_status.append(stus)
            for _, st in stus.iterrows():
                qr = {"publication-number": st["publication-number"]}
                mydoc = list(status.find(qr))
                if len(mydoc) == 0:
                    stt_id = status.insert_one(st.to_dict()).inserted_id
                else:
                    for res in mydoc:
                        diff = DeepDiff(res, st.to_dict())
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
            for _, rn in rnw.iterrows():
                qr = {"publication-number": rn["publication-number"],
                      "type-payment": rn["type-payment"],
                      "percentile": rn["percentile"],
                      "date-payment": rn["date-payment"],
                      "amount": rn["amount"]}
                mydoc = list(renewal.find(qr))
                if len(mydoc) == 0:
                    rnw_id = renewal.insert_one(rn.to_dict()).inserted_id
                else:
                    for res in mydoc:
                        diff = DeepDiff(res, rn.to_dict())
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
            for _, ert in erra.iterrows():
                qr = {"publication-number": ert["publication-number"],
                      "part": ert["part"],
                      "text": ert["text"],
                      "date-errata": ert["date-errata"],
                      "fr-bopinum": ert["fr-bopinum"]}
                mydoc = list(errata.find(qr))
                if len(mydoc) == 0:
                    err_id = errata.insert_one(ert.to_dict()).inserted_id
                else:
                    for res in mydoc:
                        diff = DeepDiff(res, ert.to_dict())
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
            for _, insc in ins.iterrows():
                qr = {"publication-number": insc["publication-number"],
                      "registered-number": insc["registered-number"],
                      "date-inscription": insc["date-inscription"],
                      "code-inscription": insc["code-inscription"],
                      "nature-inscription": insc["nature-inscription"],
                      "fr-bopinum": insc["fr-bopinum"],
                      "application-number": insc["application-number"]}
                mydoc = list(inscription.find(qr))
                if len(mydoc) == 0:
                    ins_id = inscription.insert_one(insc.to_dict()).inserted_id
                else:
                    for res in mydoc:
                        diff = DeepDiff(res, insc.to_dict())
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
                                    x = inscription.update_many(qr, nwval, upsert=True)

        if len(sear) > 0:
            liste_search.append(sear)
            for _, ser in sear.iterrows():
                qr = {"publication-number": ser["publication-number"],
                      "type-search": ser["type-search"]}
                mydoc = list(search.find(qr))
                if len(mydoc) == 0:
                    ser_id = search.insert_one(ser.to_dict()).inserted_id
                else:
                    for res in mydoc:
                        diff = DeepDiff(res, ser.to_dict())
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
                                    x = search.update_many(qr, nwval, upsert=True)

        if len(amend) > 0:
            liste_amended.append(amend)
            for _, ame in amend.iterrows():
                qr = {"publication-number": ame["publication-number"],
                      "claim": ame["claim"]}
                mydoc = list(amendedClaim.find(qr))
                if len(mydoc) == 0:
                    ame_id = amendedClaim.insert_one(ame.to_dict()).inserted_id
                else:
                    for res in mydoc:
                        diff = DeepDiff(res, ame.to_dict())
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
                                    x = amendedClaim.update_many(qr, nwval, upsert=True)

        if len(cit) > 0:
            liste_citation.append(cit)
            for _, ct in cit.iterrows():
                qr = {"publication-number": ct["publication-number"],
                      "application-number-fr": ct["application-number-fr"],
                      "type-citation": ct["type-citation"],
                      "citation": ct["citation"],
                      "country": ct["country"],
                      "doc-number": ct["doc-number"],
                      "date-doc": ct["date-doc"],
                      "passage": ct["passage"],
                      "category": ct["category"],
                      "claim": ct["claim"]
                      }
                mydoc = list(citation.find(qr))
                if len(mydoc) == 0:
                    cit_id = citation.insert_one(ct.to_dict()).inserted_id
                else:
                    for res in mydoc:
                        diff = DeepDiff(res, ct.to_dict())
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
                                    x = citation.update_many(qr, nwval, upsert=True)

        if len(prio) > 0:
            liste_priority.append(prio)
            for _, pri in prio.iterrows():
                qr = {"publication-number": pri["publication-number"],
                      "application-number-fr": pri["application-number-fr"],
                      "sequence": pri["sequence"],
                      "country": pri["country"],
                      "kind": pri["kind"],
                      "priority-number": pri["priority-number"],
                      "date-priority": pri["date-priority"]
                      }
                mydoc = list(priority.find(qr))
                if len(mydoc) == 0:
                    prio_id = priority.insert_one(pri.to_dict()).inserted_id
                else:
                    for res in mydoc:
                        diff = DeepDiff(res, pri.to_dict())
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
                                    x = priority.update_many(qr, nwval, upsert=True)

        if len(redoc) > 0:
            liste_redoc.append(redoc)
            for _, rdc in redoc.iterrows():
                qr = {"type-related-doc": rdc["type-related-doc"],
                      "country": rdc["country"],
                      "doc-number": rdc["doc-number"],
                      "date-document": rdc["date-document"],
                      "application-number-fr": rdc["application-number-fr"],
                      "publication-number": rdc["publication-number"]}
                mydoc = list(relatedDocument.find(qr))
                if len(mydoc) == 0:
                    rdc_id = relatedDocument.insert_one(rdc.to_dict()).inserted_id
                else:
                    for res in mydoc:
                        diff = DeepDiff(res, rdc.to_dict())
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
                                    x = relatedDocument.update_many(qr, nwval, upsert=True)

        if len(oldipc) > 0:
            liste_oldipc.append(oldipc)
            for _, oipc in oldipc.iterrows():
                qr = {"edition": oipc["edition"],
                      "main-classification": oipc["main-classification"],
                      "further-classification-sequence": oipc["further-classification-sequence"],
                      "further-classification": oipc["further-classification"],
                      "application-number-fr": oipc["application-number-fr"],
                      "publication-number": oipc["publication-number"]}
                mydoc = list(oldIpc.find(qr))
                if len(mydoc) == 0:
                    oipc_id = oldIpc.insert_one(oipc.to_dict()).inserted_id
                else:
                    for res in mydoc:
                        diff = DeepDiff(res, oipc.to_dict())
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
                                    nwval = {"$set": {k: oipc[k]}}
                                    x = oldIpc.update_many(qr, nwval, upsert=True)

        if len(ipcs) > 0:
            liste_ipc.append(ipcs)
            for _, ip in ipcs.iterrows():
                qr = {"classification": ip["classification"],
                      "sequence": ip["sequence"],
                      "application-number-fr": ip["application-number-fr"],
                      "publication-number": ip["publication-number"]}
                mydoc = list(ipc.find(qr))
                if len(mydoc) == 0:
                    ipc_id = ipc.insert_one(ip.to_dict()).inserted_id
                else:
                    for res in mydoc:
                        diff = DeepDiff(res, ip.to_dict())
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
                                    nwval = {"$set": {k: ip[k]}}
                                    x = ipc.update_many(qr, nwval, upsert=True)

        if len(cpcs) > 0:
            liste_cpc.append(cpcs)
            for _, cp in cpcs.iterrows():
                did = db.cpc.drop_indexes()
                qr = {"sequence": cp["sequence"],
                      "scheme": cp["scheme"],
                      "office": cp["office"],
                      "date-cpc": cp["date-cpc"],
                      "symbol": cp["symbol"],
                      "position": cp["position"],
                      "value": cp["value"],
                      "status": cp["status"],
                      "source": cp["application-number-fr"],
                      "date-classification": cp["application-number-fr"],
                      "application-number-fr": cp["application-number-fr"],
                      "publication-number": cp["publication-number"]}
                mydoc = list(cpc.find(qr))
                if len(mydoc) == 0:
                    cpc_id = cpc.insert_one(cp.to_dict()).inserted_id
                else:
                    for res in mydoc:
                        diff = DeepDiff(res, cp.to_dict())
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
                                    did2 = db.cpc.drop_indexes()
                                    nwval = {"$set": {k: cp[k]}}
                                    x = cpc.update_many(qr, nwval, upsert=True)

    liste_pub = []
    for document in publication.find():
        liste_pub.append(document)
        # print(document)

    df_pub = pd.DataFrame(liste_pub)

    liste_applicants = []
    for document in person.find():
        liste_applicants.append(document)
        # print(document)

    df_app = pd.DataFrame(liste_applicants)

    liste_pubref = []
    for document in publicationRef.find():
        liste_pubref.append(document)
        # print(document)

    df_pubref = pd.DataFrame(liste_pubref)

    liste_appref = []
    for document in application.find():
        liste_appref.append(document)
        # print(document)

    df_appref = pd.DataFrame(liste_appref)

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
    for document in inscription.find():
        liste_inscriptions.append(document)
        # print(document)

    df_ins = pd.DataFrame(liste_inscriptions)

    liste_search = []
    for document in search.find():
        liste_search.append(document)
        # print(document)

    df_search = pd.DataFrame(liste_search)

    liste_amend = []
    for document in amendedClaim.find():
        liste_amend.append(document)
        # print(document)

    df_amended = pd.DataFrame(liste_amend)

    liste_cit = []
    for document in citation.find():
        liste_cit.append(document)
        # print(document)

    df_citations = pd.DataFrame(liste_cit)

    liste_prio = []
    for document in priority.find():
        liste_prio.append(document)
        # print(document)

    df_prio = pd.DataFrame(liste_prio)

    liste_rdc = []
    for document in relatedDocument.find():
        liste_rdc.append(document)
        # print(document)

    df_rdc = pd.DataFrame(liste_rdc)

    liste_oipc = []
    for document in oldIpc.find():
        liste_oipc.append(document)
        # print(document)

    df_oipc = pd.DataFrame(liste_oipc)

    liste_ic = []
    for document in ipc.find():
        liste_ic.append(document)
        # print(document)

    df_ipc = pd.DataFrame(liste_ic)

    liste_cp = []
    for document in cpc.find():
        liste_cp.append(document)
        # print(document)

    df_cpc = pd.DataFrame(liste_cp)

    client.close()
