from es_constant.constants import AUTHOR_DOCUMENT_INDEX
from es_service.es_helpers.es_connection import elasticsearch_connection

from es_service.es_search.es_search_helpers import get_author_default_sort
from es_service.es_search.es_search_helpers import get_author_default_source


def get_author_by_id(es, index, author_id):
    query = {
        "query": {
            "match": {
                "authorId": author_id
            }
        }
    }
    result = es.search(index=index, body=query)
    print("Author by id:", result)
    return result["hits"]["hits"]


def get_author_by_name(es, index, name):
    query = {
        "query": {
            "multi_match": {
                "query": name,
                "fields": [
                    "aliases",
                    "name"
                ]
            }
        },
        "_source": [
            "name",
            "aliases"
        ]
    }
    result = es.search(index=index, body=query)
    print('Get author by name and aliases', result)
    return result['hits']['hits']


if __name__ == "__main__":
    get_author_by_name(elasticsearch_connection, AUTHOR_DOCUMENT_INDEX, 'wang')
    get_author_by_id(elasticsearch_connection, AUTHOR_DOCUMENT_INDEX, '120789427')
