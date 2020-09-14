from es_constant.constants import AUTHOR_DOCUMENT_INDEX
from es_service.es_helpers.es_connection import elasticsearch_connection

from es_service.es_search.es_search_helpers import get_author_default_sort
from es_service.es_search.es_search_helpers import get_author_default_source

from elasticsearch import NotFoundError


def get_all_authors(es, index, start=0, size=0):
    query = {
        "query": {
            "match_all": {}
        },
        "from": start,
        "size": size
    }
    res = es.search(index=index, body=query)
    print("Get all authors result :", res)
    return res["hits"]


def get_author_by_id(es, index, author_id):
    try:
        author = es.get(index=index, id=author_id)
        result = {
                "aliases":	author['_source']["aliases"],
                "authorId":	author['_source']["authorId"],
                "influentialCitationCount":	author['_source']["influentialCitationCount"],
                "totalPapers": author['_source']["influentialCitationCount"],
                "name":	author['_source']["name"],
                "papers": author['_source']["papers"][:5]
            }
        return result
    except NotFoundError:
        print('author_id {} not found'.format(author_id))
        return {}


def get_some_papers(es, index, author_id, start=5, size=5):
    try:
        author = es.get(index=index, id=author_id)
        print("get_some_papers result: ", author["_source"]["papers"][start:start + size])
        return author["_source"]["papers"][start:start + size]
    except NotFoundError:
        print('author {} not found'.format(author_id))
        return {}


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
    print(get_author_by_id(elasticsearch_connection, "author_test", 52098299))
