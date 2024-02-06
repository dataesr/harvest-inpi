import os
import pandas as pd
from bs4 import BeautifulSoup
from timeit import default_timer as timer

import inpi.p02_lecture_xml as lec
from utils.utils import print_progress

from application.server.main.logger import get_logger

logger = get_logger(__name__)

DATA_PATH = os.getenv("MOUNTED_VOLUME_TEST")
XML_PATTERN = "*.xml"
NEW_PATTERN = "FR_FRNEW"


def extract_xml(file: str, data_xml: str):
    """Extract xml data from a file.

    Args:
        file (str): _description_
        data_xml (str): _description_

    Returns:
        _type_: _description_
    """
    os.chdir(DATA_PATH)

    list_dir = os.listdir(DATA_PATH)
    list_dir.sort()

    elem_file = file.split("/")
    if len(data_xml) > 0:
        bs_data = BeautifulSoup(data_xml, "xml")
        patent = bs_data.find("fr-patent-document")
        date_produced = lec.date_pub_ref(elem_file, patent)
        publication_number = lec.doc_nb(elem_file, patent)

        stats = lec.stat_pub(elem_file, patent)

        publication = {
            "lang": patent["lang"],
            "application-number-fr": patent["id"],
            "country": patent["country"],
            "date-produced": date_produced,
            "publication-number": publication_number,
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

        extension = bs_data.find("fr-extension")
        if extension:
            extension_territory = extension.find("fr-extension-territory")
            if extension_territory:
                publication["fr-extension-territory"] = extension_territory.text

        invention_title = bs_data.find("invention-title")
        if invention_title:
            publication["title"] = invention_title.text.lstrip().rstrip()

        abstract = bs_data.find("abstract")
        if abstract:
            publication["abstract"] = abstract.text.lstrip().rstrip()

        person = lec.person_ref(bs_data, publication_number, patent)

        publicationRef = lec.pub_ref(bs_data, publication_number, patent)

        application = lec.app_ref(bs_data, publication_number, patent)

        patent_life = bs_data.find("fr-patent-life")

        if patent_life:
            granted = patent_life.find("fr-date-granted")
            if granted:
                tags_item = [tag.name for tag in granted.find_all()]
                if "kind" in tags_item:
                    kind = granted.find("kind")
                    if kind:
                        publication["kind-grant"] = kind.text
                if "fr-bopinum" in tags_item:
                    bopinum = granted.find("fr-bopinum")
                    if bopinum:
                        publication["fr-bopinum-grant"] = bopinum.text
                if "date" in tags_item:
                    date = granted.find("date")
                    if date:
                        publication["date-grant"] = lec.check_date(date.text)

            refused = patent_life.find("fr-date-application-refused")
            if refused:
                date = refused.find("date")
                if date:
                    publication["date-refusal"] = lec.check_date(date.text)

            withdrawn = patent_life.find("fr-date-application-withdrawn")
            if withdrawn:
                date = withdrawn.find("date")
                if date:
                    publication["date-withdrawal"] = lec.check_date(date.text)

            lapsed = patent_life.find("fr-date-notification-lapsed")
            if lapsed:
                date = lapsed.find("date")
                if date:
                    publication["date-lapsed"] = lec.check_date(date.text)
                bopinum = lapsed.find("fr-bopinum")
                if bopinum:
                    publication["fr-bopinum-lapsed"] = bopinum.text

            status = patent_life.find("fr-status")
            if status:
                nature = status.find("fr-nature")
                if nature:
                    publication["fr-nature"] = nature.text

            renewal = lec.renewal_list(patent_life, publication_number, patent)

            dic_errata = {
                "publication-number": publication_number,
                "part": "",
                "text": "",
                "date-errata": "",
                "fr-bopinum": "",
                "application-number": patent["id"],
            }

            errata = lec.errata_list(patent_life, dic_errata)

            dic_inscription = {
                "publication-number": publication_number,
                "registered-number": "",
                "date-inscription": "",
                "code-inscription": "",
                "nature-inscription": "",
                "fr-bopinum": "",
                "application-number": patent["id"],
            }

            inscription = lec.inscr_list(patent_life, dic_inscription)

            search = lec.search_list(patent_life, publication_number, patent)

            dic_amendedClaim = {
                "publication-number": publication_number,
                "claim": "",
                "application-number": patent["id"],
            }

            amendedClaim = lec.amended_list(patent_life, dic_amendedClaim)

            dic_citation = {
                "type-citation": "",
                "citation": "",
                "country": "",
                "doc-number": "",
                "date-doc": "",
                "passage": "",
                "category": "",
                "claim": "",
                "application-number-fr": patent["id"],
                "publication-number": publication_number,
            }

            citation = lec.cit_list(patent_life, dic_citation)

        else:
            renewal = pd.DataFrame(
                data=[
                    {
                        "publication-number": publication_number,
                        "type-payment": "",
                        "percentile": "",
                        "date-payment": "",
                        "amount": "",
                        "application-number-fr": patent["id"],
                    }
                ]
            )

            errata = pd.DataFrame(
                data=[
                    {
                        "publication-number": publication_number,
                        "part": "",
                        "text": "",
                        "date-errata": "",
                        "fr-bopinum": "",
                        "application-number": patent["id"],
                    }
                ]
            ).drop_duplicates()

            inscription = pd.DataFrame(
                data=[
                    {
                        "publication-number": publication_number,
                        "registered-number": "",
                        "date-inscription": "",
                        "code-inscription": "",
                        "nature-inscription": "",
                        "fr-bopinum": "",
                        "application-number": patent["id"],
                    }
                ]
            ).drop_duplicates()

            search = pd.DataFrame(
                data=[
                    {
                        "publication-number": publication_number,
                        "type-search": "",
                        "date-search": "",
                        "fr-bopinum": "",
                        "application-number-fr": patent["id"],
                    }
                ]
            ).drop_duplicates()

            amendedClaim = pd.DataFrame(
                data=[{"publication-number": publication_number, "claim": "", "application-number": patent["id"]}]
            ).drop_duplicates()

            citation = pd.DataFrame(
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
                        "application-number-fr": patent["id"],
                        "publication-number": publication_number,
                    }
                ]
            ).drop_duplicates()

        publication = pd.DataFrame(data=[publication]).drop_duplicates()

        priority = lec.prio_list(bs_data, publication_number, patent)

        relatedDocument = lec.redoc_list(bs_data, publication_number, patent)

        oldIpc = lec.oldipc_list(bs_data, publication_number, patent)

        ipc = lec.ipc_list(bs_data, publication_number, patent)

        cpc = lec.cpc_list(bs_data, publication_number, patent)

    else:
        collections = {}
        logger.warn(f"Xml empty for file {file}")
        return collections

    collections = {
        "amendedClaim": amendedClaim.to_dict("record"),
        "application": application.to_dict("record"),
        "citation": citation.to_dict("record"),
        "cpc": cpc.to_dict("record"),
        "errata": errata.to_dict("record"),
        "inscription": inscription.to_dict("record"),
        "ipc": ipc.to_dict("record"),
        "oldIpc": oldIpc.to_dict("record"),
        "person": person.to_dict("record"),
        "priority": priority.to_dict("record"),
        "publication": publication.to_dict("record"),
        "publicationRef": publicationRef.to_dict("record"),
        "relatedDocument": relatedDocument.to_dict("record"),
        "renewal": renewal.to_dict("record"),
        "search": search.to_dict("record"),
    }

    # logger.debug(f"{publication_number} collection:")
    # logger.debug(f"{collections}")

    return collections


def extract_file(file):
    """Read a file and extract xml data.

    Args:
        file (_type_): _description_

    Returns:
        _type_: _description_
    """
    collections = {}

    # logger.debug(f"Extract file {file}")
    with open(file, "r") as f:
        xml = f.read()

    collections = extract_xml(file, xml)

    return collections


def extract(files, show_progress=False):
    """Extract data from a list of files.

    Args:
        files (_type_): _description_
        show_progress (bool, optional): _description_. Defaults to False.

    Returns:
        _type_: _description_
    """
    collections = []
    extract_timer = timer()
    count_timer = timer()

    files_total = len(files)
    logger.info(f"Start extract of {files_total} files")

    for count, file in enumerate(files):
        if show_progress and (timer() - count_timer) > 60:
            count_timer = timer()
            logger.info(print_progress(count, files_total))

        collections.append(extract_file(file))

    if show_progress:
        logger.info(print_progress(files_total, files_total))

    logger.info(f"Extract done in {(timer() - extract_timer):.2f}")

    return collections
