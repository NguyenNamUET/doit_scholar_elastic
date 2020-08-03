from elasticsearch import Elasticsearch
from es_service.es_helpers.es_connection import elasticsearch_connection


def insert_doc(es, index, id, body, routing=None, verbose=True):
    if routing is not None:
        res = es.index(index=index, id=id, body=body, routing=routing)
    else:
        res = es.index(index=index, id=id, body=body)

    es.indices.refresh(index=index)
    if verbose:
        print(res)
    return True


def create_index(es, index, mapping, verbose=True):
    print (index)
    res = es.indices.create(index=index, ignore=400, body=mapping)
    if verbose:
        print(res)
        print("Success")
    return True


if __name__ == "__main__":
    from es_constant.constants import PAPER_MAPPING, PAPER_DOCUMENT_INDEX

    create_index(elasticsearch_connection, PAPER_DOCUMENT_INDEX, PAPER_MAPPING)