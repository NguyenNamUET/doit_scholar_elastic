if __name__ == "__main__":
    from es_service.es_helpers.es_connection import elasticsearch_connection
    from es_constant.constants import PAPER_MAPPING, PAPER_DOCUMENT_INDEX
    from es_service.es_helpers.es_operator import create_index
    create_index(elasticsearch_connection, PAPER_DOCUMENT_INDEX, PAPER_MAPPING)

    from es_service.es_index.es_papers_index import index_papers
    index_papers()

    from es_service.es_index.es_authors_index import index_authors
    index_authors()

