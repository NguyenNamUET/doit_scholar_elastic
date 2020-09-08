from es_service.es_helpers.utilites import load_json, write_to_record
from es_constant.constants import PAPER_DOCUMENT_INDEX, MAPPING_DOCUMENT_INDEX
from es_service.es_helpers.es_connection import elasticsearch_connection
from es_service.es_helpers.es_operator import insert_doc
from es_service.es_helpers.utilites import crawl_base_sitemap, crawl_second_sitemap, get_paper_api_v2, extract_url_id

import os
import math
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor


def index_paper_document(paper_id):
    paper_document = get_paper_api_v2(paper_id)
    try:
        paper_document["citations_count"] = len(paper_document["citations"])
        paper_document["references_count"] = len(paper_document["citations"])
        insert_doc(es=elasticsearch_connection, index="paper_test",
                   id=paper_document["paperId"], body=paper_document)
        print("Success")
        return paper_document
    except Exception as e:
        print("Paper {} index error: {}".format(paper_id, e))


def index_papers():
    paper_sitemaps_list = crawl_base_sitemap("https://www.semanticscholar.org/sitemap_paper_index.xml")
    for paper_sitemap in paper_sitemaps_list[:1]:
        executor = ThreadPoolExecutor(max_workers=20)
        paper_urls = crawl_second_sitemap(paper_sitemap)
        if paper_urls is not None:
            for paper_url in paper_urls[:100]:
                paper_id = extract_url_id(paper_url)
                executor.submit(index_paper_document, paper_id)

