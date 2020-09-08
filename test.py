from es_service.es_helpers.es_connection import elasticsearch_connection
from es_service.es_helpers.es_operator import insert_doc, create_index
from es_service.es_index.es_authors_index import index_author_document
from es_service.es_index.es_papers_index import index_paper_document


from es_service.es_helpers.utilites import extract_url_id, crawl_base_sitemap, crawl_second_sitemap, \
    get_paper_api_v2, grouper

from es_constant.constants import PAPER_TEST_MAPPING

import concurrent.futures


def index_documents(author_url):
    author_id = extract_url_id(author_url)
    try:
        # insert_doc(es=elasticsearch_connection, index="paper_test",
        #            id=paper_id, body=paper_document)
        author_document = index_author_document(author_id)

        if len(author_document["papers"]) > 0:
            for paper in author_document["papers"]:
                index_paper_document(paper["paperId"])
                print("Success paper")

        print("Success author")

    except Exception as e:
        print("Author {} index error: {}".format(author_id, e))

    return author_id


def index_data():
    author_sitemaps_list = crawl_base_sitemap("https://www.semanticscholar.org/sitemap_author_index.xml")
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        for author_sitemap in author_sitemaps_list[:20]:
            author_urls = crawl_second_sitemap(author_sitemap)
            if author_urls is not None:
                for index, urls_group in enumerate(grouper(author_urls, 1000)):
                    future_to_url = {executor.submit(index_documents, author_url): author_url for author_url in urls_group if author_url is not None}
                    for future in concurrent.futures.as_completed(future_to_url):
                        author_url = future_to_url[future]
                        try:
                            author_id = future.result()
                        except Exception as exc:
                            print('%r generated an exception: %s' % (author_url, exc))


if __name__ == "__main__":
    #create_index(elasticsearch_connection, "paper_test", PAPER_TEST_MAPPING)
    index_data()

