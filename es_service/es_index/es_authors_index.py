from es_service.es_helpers.utilites import load_json, extract_url_id, get_author_api, \
    crawl_base_sitemap, crawl_second_sitemap
from es_constant.constants import AUTHOR_DOCUMENT_INDEX, AUTHORS_DATA_PATH
from es_service.es_helpers.es_connection import elasticsearch_connection
from es_service.es_helpers.es_operator import insert_doc

import os
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor


def index_author_document(author_id):
    author_document = get_author_api(author_id)
    try:
        index_document = {"authorId": author_document["authorId"],
                          "aliases": author_document["aliases"],
                          "name": author_document["name"],
                          "influentialCitationCount": author_document["influentialCitationCount"],
                          "totalPapers": len(author_document["papers"]),
                          "papers": author_document["papers"]
                          }

        insert_doc(es=elasticsearch_connection, index="author_test", id=author_id, body=index_document, verbose=True)
        print("Success author")

        return index_document
    except Exception as e:
        print("Author {} index error: {}".format(author_id, e))


def index_authors():
    author_sitemaps_list = crawl_base_sitemap("https://www.semanticscholar.org/sitemap_author_index.xml")

    for author_sitemap in author_sitemaps_list[:1]:
        executor = ThreadPoolExecutor(max_workers=20)
        author_urls = crawl_second_sitemap(author_sitemap)
        if author_urls is not None:
            for author_url in author_urls[:100]:
                author_id = extract_url_id(author_url)
                executor.submit(index_author_document, author_id)

