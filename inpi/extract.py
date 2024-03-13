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


def extract_xml(file: str, data_xml: str, extract_collections: list) -> dict:
    """Extract xml data from a file.

    Args:
        file: File name.
        data_xml: File xml.
        extract_collections: Collections to extract from xml.

    Returns:
        Collections data
    """
    os.chdir(DATA_PATH)

    list_dir = os.listdir(DATA_PATH)
    list_dir.sort()
    
    collections = {}

    if len(data_xml) > 0:
        
        # Get xml data
        elem_file = file.split("/")
        bs_data = BeautifulSoup(data_xml, "xml")
        patent = bs_data.find("fr-patent-document")
        patent_life = bs_data.find("fr-patent-life")
        publication_number = lec.doc_nb(elem_file, patent)
        
        # Fill publication collection
        if "publication" in extract_collections:
            date_produced = lec.date_pub_ref(elem_file, patent)
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

            collections["publication"] = publication
        
        # Fill person collection
        if "person" in extract_collections:
            person = lec.person_ref(bs_data, publication_number, patent)
            collections["person"] = person.to_dict("record")

        # Fill publicationRef collection
        if "publicationRef" in extract_collections:
            publicationRef = lec.pub_ref(bs_data, publication_number, patent)
            collections["publicationRef"] = publicationRef.to_dict("record")
        
        # Fill application collection
        if "application" in extract_collections:
            application = lec.app_ref(bs_data, publication_number, patent)
            collections["application"] = application.to_dict("record")

        # Fill renewal collection
        if "renewal" in extract_collections:
            dic_renewal = {
                    "publication-number": publication_number,
                    "type-payment": "",
                    "percentile": "",
                    "date-payment": "",
                    "amount": "",
                    "application-number-fr": patent["id"],
                }
            if patent_life:
                renewal = lec.renewal_list(patent_life, publication_number, patent)
                collections["renewal"] = renewal.to_dict("record")
                
            else:
                collections["renewal"] = dic_renewal

        # Fill errata collection 
        if "errata" in extract_collections:
            dic_errata = {
                "publication-number": publication_number,
                "part": "",
                "text": "",
                "date-errata": "",
                "fr-bopinum": "",
                "application-number": patent["id"],
            }
            if patent_life:
                errata = lec.errata_list(patent_life, dic_errata)
                collections["errata"] = errata.to_dict("record")
            else:
                collections["errata"] = dic_errata

        # Fill inscription collection
        if "inscription" in extract_collections:
            dic_inscription = {
                "publication-number": publication_number,
                "registered-number": "",
                "date-inscription": "",
                "code-inscription": "",
                "nature-inscription": "",
                "fr-bopinum": "",
                "application-number": patent["id"],
            }
            if patent_life:
                inscription = lec.inscr_list(patent_life, dic_inscription)
                collections["inscription"] = inscription.to_dict("record")
            else:
                collections["inscription"] = dic_inscription

        # Fill search collection
        if "search" in extract_collections:
            dic_search = {
                        "publication-number": publication_number,
                        "type-search": "",
                        "date-search": "",
                        "fr-bopinum": "",
                        "application-number-fr": patent["id"],
                    }
            if patent_life:
                search = lec.search_list(patent_life, publication_number, patent)
                collections["search"] = search.to_dict("record")
            else:
                collections["search"] = dic_search

        # Fill amendedClaim collection
        if "amendedClaim" in extract_collections:
            dic_amendedClaim = {
                "publication-number": publication_number,
                "claim": "",
                "application-number": patent["id"],
            }
            if patent_life:
                amendedClaim = lec.amended_list(patent_life, dic_amendedClaim)
                collections["amendedClaim"] = amendedClaim.to_dict("record")
            else:
                collections["amendedClaim"] = dic_amendedClaim
                
        # Fill citation collection
        if "citation" in extract_collections:
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
            if patent_life:
                citation = lec.cit_list(patent_life, dic_citation)
                collections["citation"] = citation.to_dict("record")
            else:
                collections["citation"] = dic_citation

        # Fill priority collection 
        if "priority" in extract_collections:
            priority = lec.prio_list(bs_data, publication_number, patent)
            collections["priority"] = priority.to_dict("record")

        # Fill relatedDocument collection
        if "relatedDocument" in extract_collections:
            relatedDocument = lec.redoc_list(bs_data, publication_number, patent)
            collections["oldIpc"] = relatedDocument.to_dict("record")

        # Fill oldIpc collection
        if "oldIpc" in extract_collections:
            oldIpc = lec.oldipc_list(bs_data, publication_number, patent)
            collections["oldIpc"] = oldIpc.to_dict("record")

        # Fill ipc collection
        if "ipc" in extract_collections:
            ipc = lec.ipc_list(bs_data, publication_number, patent)
            collections["ipc"] = ipc.to_dict("record")

        # Fill cpc collection
        if "cpc" in extract_collections:
            cpc = lec.cpc_list(bs_data, publication_number, patent)
            collections["cpc"] = cpc.to_dict("record")

    else:
        logger.warn(f"Xml empty for file {file}")
        return collections

    # logger.debug(f"{publication_number} collection:")
    # logger.debug(f"{collections}")

    return collections


def extract_file(file:str, extract_collections:list) -> dict:
    """Read a file and extract xml data.

    Args:
        file: File name.
        extract_collections:  Collections to extract from xml.

    Returns:
        Collections data.
    """
    collections = {}

    # logger.debug(f"Extract file {file}")
    with open(file, "r") as f:
        xml = f.read()

    collections = extract_xml(file, xml, extract_collections)

    return collections


def extract(files:list, extract_collections:list, show_progress=False) -> list:
    """Extract data from a list of files.

    Args:
        files: List of files.
        extract_collections: Collections to extract from xml.
        show_progress (optional): Should progress of extraction be displayed. Defaults to False.

    Returns:
        Collections data.
    """
    collections = []
    extract_timer = timer()
    count_timer = timer()

    files_total = len(files)

    for count, file in enumerate(files):
        if show_progress and (timer() - count_timer) > 60:
            count_timer = timer()
            logger.info(print_progress(count, files_total))

        collections.append(extract_file(file, extract_collections))

    if show_progress:
        logger.info(print_progress(files_total, files_total))

    logger.info(f"Extract done in {(timer() - extract_timer):.2f}")

    return collections
