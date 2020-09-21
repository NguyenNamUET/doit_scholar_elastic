from crawl_service.localstorage.s2_firstdownload import download_data
from es_service.es_index.es_papers_index import index_papers
from es_service.es_helpers.es_operator import create_index
from es_service.es_constant.constants import PAPER_DOCUMENT_INDEX, PAPER_MAPPING
from es_service.es_helpers.es_connection import elasticsearch_connection
if __name__ == "__main__":
    #download_data(0)
    create_index(es=elasticsearch_connection, index=PAPER_DOCUMENT_INDEX, mapping=PAPER_MAPPING)
    index_papers()

