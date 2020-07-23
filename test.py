from elasticsearch import Elasticsearch, NotFoundError
import json

ES_IP = 'localhost'
ES_USER = 'user'
ES_PASS = '12345678'
ES_PORT = '9202'
INDEX_LAW = 'doit_test'
TYPE_DOCUMENT = '_doc'

PATH = "/home/nguyennam/Downloads/Semantic/sample/data/metadata/sample.jsonl"


def check_status_es(es):
    if not es.ping():
        raise ValueError("Connection failed")
    else:
        print('ES live', es)
    return True


def insert_doc(es, index, doc_type, id, body, verbose=True):
    res = es.index(index=index, doc_type=doc_type, id=id, body=body)
    es.indices.refresh(index=index)
    if verbose:
        print(res)
    return True


def load_jsonl(path):
    with open(path, "r") as json_file:
        json_objects = [json.loads(jsonline) for jsonline in json_file.readlines()]

    return json_objects


if __name__ == "__main__":
    elasticsearch_connection = Elasticsearch(
        ['http://' + ES_USER + ':' + ES_PASS + '@' + ES_IP + ':' + ES_PORT],
        verify_certs=False, timeout=30)  # http://user:12345678@localhost:9202
    check_status_es(elasticsearch_connection)