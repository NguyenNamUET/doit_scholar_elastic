from elasticsearch import Elasticsearch
from elasticsearch import AsyncElasticsearch
from es_constant.constants import ES_USER, ES_PASS, ES_IP, ES_PORT


elasticsearch_connection = Elasticsearch(
    ['http://' + ES_USER + ':' + ES_PASS + '@' + ES_IP + ':' + ES_PORT],
    verify_certs=False, timeout=30)  # http://user:12345678@localhost:9202

elasticsearch_connection_async = AsyncElasticsearch(
    ['http://' + ES_USER + ':' + ES_PASS + '@' + ES_IP + ':' + ES_PORT],
    verify_certs=False, timeout=30)


def check_status_es(es):
    if not es.ping():
        raise ValueError("Connection failed")
    else:
        print('ES live', es)
    return True


if __name__ == "__main__":
    check_status_es(elasticsearch_connection)