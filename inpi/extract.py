import os
import pandas as pd

import pandas as pd
from bs4 import BeautifulSoup

import inpi.p02_lecture_xml as lec

from application.server.main.logger import get_logger

logger = get_logger(__name__)

DATA_PATH = os.getenv("MOUNTED_VOLUME_TEST")


def extract_xml(file_name: str, data_xml: str):
    os.chdir(DATA_PATH)

    list_dir = os.listdir(DATA_PATH)
    list_dir.sort()

    elem_file = file_name.split("/")
    if len(data_xml) > 0:
        bs_data = BeautifulSoup(data_xml, "xml")
        pn = bs_data.find("fr-patent-document")
        date_produced = lec.date_pub_ref(elem_file, pn)
        pub_n = lec.doc_nb(elem_file, pn)

        stats = lec.stat_pub(elem_file, pn)

        dic_pn = {
            "lang": pn["lang"],
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
            "fr-bopinum-lapsed": "",
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

        appl = lec.person_ref(bs_data, pub_n, pn)

        pref = lec.pub_ref(bs_data, pub_n, pn)

        aref = lec.app_ref(bs_data, pub_n, pn)

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
                    dic_pn["date-grant"] = lec.check_date(dae)

            ref = ptlife.find("fr-date-application-refused")

            if ref:
                dae = ref.find("date").text
                dic_pn["date-refusal"] = lec.check_date(dae)

            wd = ptlife.find("fr-date-application-withdrawn")

            if wd:
                dae = wd.find("date").text
                dic_pn["date-withdrawal"] = lec.check_date(dae)

            lp = ptlife.find("fr-date-notification-lapsed")

            if lp:
                dae = lp.find("date").text
                dic_pn["date-lapsed"] = lec.check_date(dae)
                dic_pn["fr-bopinum-lapsed"] = lp.find("fr-bopinum").text

            stt = ptlife.find("fr-status")

            if stt:
                dic_pn["fr-nature"] = stt.find("fr-nature").text

            rnw = lec.renewal_list(ptlife, pub_n, pn)

            dic_errata = {
                "publication-number": pub_n,
                "part": "",
                "text": "",
                "date-errata": "",
                "fr-bopinum": "",
                "application-number": pn["id"],
            }

            erra = lec.errata_list(ptlife, dic_errata)

            dic_ins = {
                "publication-number": pub_n,
                "registered-number": "",
                "date-inscription": "",
                "code-inscription": "",
                "nature-inscription": "",
                "fr-bopinum": "",
                "application-number": pn["id"],
            }

            ins = lec.inscr_list(ptlife, dic_ins)

            sear = lec.search_list(ptlife, pub_n, pn)

            dic_amended = {"publication-number": pub_n, "claim": "", "application-number": pn["id"]}

            amend = lec.amended_list(ptlife, dic_amended)

            dic_citations = {
                "type-citation": "",
                "citation": "",
                "country": "",
                "doc-number": "",
                "date-doc": "",
                "passage": "",
                "category": "",
                "claim": "",
                "application-number-fr": pn["id"],
                "publication-number": pub_n,
            }

            cit = lec.cit_list(ptlife, dic_citations)

        else:
            rnw = pd.DataFrame(
                data=[
                    {
                        "publication-number": pub_n,
                        "type-payment": "",
                        "percentile": "",
                        "date-payment": "",
                        "amount": "",
                        "application-number-fr": pn["id"],
                    }
                ]
            )

            erra = pd.DataFrame(
                data=[
                    {
                        "publication-number": pub_n,
                        "part": "",
                        "text": "",
                        "date-errata": "",
                        "fr-bopinum": "",
                        "application-number": pn["id"],
                    }
                ]
            ).drop_duplicates()

            ins = pd.DataFrame(
                data=[
                    {
                        "publication-number": pub_n,
                        "registered-number": "",
                        "date-inscription": "",
                        "code-inscription": "",
                        "nature-inscription": "",
                        "fr-bopinum": "",
                        "application-number": pn["id"],
                    }
                ]
            ).drop_duplicates()

            sear = pd.DataFrame(
                data=[
                    {
                        "publication-number": pub_n,
                        "type-search": "",
                        "date-search": "",
                        "fr-bopinum": "",
                        "application-number-fr": pn["id"],
                    }
                ]
            ).drop_duplicates()

            amend = pd.DataFrame(
                data=[{"publication-number": pub_n, "claim": "", "application-number": pn["id"]}]
            ).drop_duplicates()

            cit = pd.DataFrame(
                data=[
                    {
                        "type-citation": "",
                        "citation": "",
                        "country": "",
                        "doc-number": "",
                        "date-doc": "",
                        "passage": "",
                        "category": "",
                        "claim": "",
                        "application-number-fr": pn["id"],
                        "publication-number": pub_n,
                    }
                ]
            ).drop_duplicates()

        dic_pn = pd.DataFrame(data=[dic_pn]).drop_duplicates()

        prio = lec.prio_list(bs_data, pub_n, pn)

        redoc = lec.redoc_list(bs_data, pub_n, pn)

        oldipc = lec.oldipc_list(bs_data, pub_n, pn)

        ipcs = lec.ipc_list(bs_data, pub_n, pn)

        cpcs = lec.cpc_list(bs_data, pub_n, pn)

    else:
        collections = {}
        logger.warn(f"Xml empty for file {file_name}")
        return collections

    collections = {"dic_pn": dic_pn.to_dict("record"), "pref": pref.to_dict("record")}
    logger.debug(f"{pub_n} collection:")
    logger.debug(f"{collections}")

    return collections


def extract_file(file):
    collections = {}

    logger.debug(f"Extract file {file}")
    with open(file, "r") as f:
        xml = f.read()

    collections = extract_xml(file, xml)

    return collections


def extract(files):
    CHUNK_SIZE = 5
    chunk_collections = []
    chunk = files

    if len(files) > CHUNK_SIZE:
        chunk = files[:CHUNK_SIZE]

    logger.debug(f"Start extract of {len(chunk)} files")
    for file in chunk:
        chunk_collections.append(extract_file(file))

    return chunk_collections
