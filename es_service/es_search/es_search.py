from elasticsearch import NotFoundError

from es_constant.constants import PAPER_DOCUMENT_INDEX
from es_service.es_helpers.es_connection import elasticsearch_connection

from es_service.es_search.es_search_helpers import get_paper_default_source, get_paper_aggregation_of_authors, \
    get_paper_aggregation_of_fields_of_study, get_paper_default_sort


def get_all_fields_of_study(es, index):
    query = {
        "size": 0,
        "aggs": {
            "fields_of_study": get_paper_aggregation_of_fields_of_study()
        }
    }

    result = es.search(index=index, body=query)
    print("Get all fields of study result :", query)
    return result["aggregations"]["fields_of_study"]["buckets"]


def get_paper_by_id(es, index, id):
    try:
        res = es.get(index=index, id=id)
        return res['_source']
    except NotFoundError:
        print('not found')
        return {}


def get_all_papers(es, index, start=0, size=10, source=None):
    if source is None:
        source = get_paper_default_source()

    query = {
        "query": {
            "match_all": {}
        },
        "from": start,
        "size": size,
        "aggs": {
            "fields_of_study": get_paper_aggregation_of_fields_of_study()
        },
        "_source": source
    }

    result = es.search(index=index, body=query)
    print("Get all papers result :", result)
    return result["hits"]["hits"]


def search_paper_title(search_content, es, index, start=0, size=10, source=None, sort_by=None,
                       return_top_author=False, top_author_size=10):
    if source is None:
        source = get_paper_default_source()

    if sort_by is None:
        sort = get_paper_default_sort()

    if return_top_author:
        if return_top_author:
            query = {
                "query": {
                    "match": {
                        "title": {
                            "query": search_content,
                            "fuzziness": 1
                        }
                    }
                },
                "from": start,
                "size": size,
                "aggs": {
                    "author_count": get_paper_aggregation_of_authors(size=top_author_size),
                    "fields_of_study": get_paper_aggregation_of_fields_of_study()
                },
                "_source": source,
                "sort": [sort]
            }
    else:
        query = {
            "query": {
                "match": {
                    "title": {
                        "query": search_content,
                        "fuzziness": 1
                    }
                }
            },
            "from": start,
            "size": size,
            "aggs": {
                "fields_of_study": get_paper_aggregation_of_fields_of_study()
            },
            "_source": source,
            "sort": [sort]
        }
    print("Query: ", query)
    result = es.search(index=index, body=query)
    print("Search paper title :", result)
    if result["hits"]["total"]["value"] == 0:
        return {}

    return result


def search_paper_abstract(search_content, es, index, start, size, source=None, sort_by=None,
                          return_top_author=False, top_author_size=10):
    if source is None:
        source = get_paper_default_source()

    if sort_by is None:
        sort = get_paper_default_sort()

    if return_top_author:
        query = {
            "query": {
                "match": {
                    "abstract": {
                        "query": search_content,
                        "fuzziness": 1
                    }
                }
            },
            "from": start,
            "size": size,
            "aggs": {
                "author_count": get_paper_aggregation_of_authors(size=top_author_size),
                "fields_of_study": get_paper_aggregation_of_fields_of_study()
            },
            "_source": source,
            "sort": [sort]
        }
    else:
        query = {
            "query": {
                "match": {
                    "abstract": {
                        "query": search_content,
                        "fuzziness": 1
                    }
                }
            },
            "from": start,
            "size": size,
            "aggs": {
                "fields_of_study": get_paper_aggregation_of_fields_of_study()
            },
            "_source": source,
            "sort": [sort]
        }

    result = es.search(index=index, body=query)
    print("Search paper abstract :", result)
    if result["hits"]["total"]["value"] == 0:
        return {}

    return result


def get_paper_by_topic(es, index, topics, start=0, size=10, source=None, sort_by=None, is_should=True):
    if source is None:
        source = get_paper_default_source()

    if sort_by is None:
        sort = get_paper_default_sort()

    if is_should:
        query = {
            "query": {
                "bool": {
                    "should": []
                }
            },
            "from": start,
            "size": size,
            "_source": source,
            "sort": [sort]
        }
        for topic in topics:
            query["query"]["bool"]["should"].append({
                            "match": {
                                "topics.topicId": {
                                    "query": topic
                                }
                            }
                        })
    else:
        query = {
            "query": {
                "bool": {
                    "must": []
                }
            },
            "from": start,
            "size": size,
            "_source": source,
            "sort": [sort]
        }
        for topic in topics:
            query["query"]["bool"]["must"].append({
                "match": {
                    "topics.topicId": {
                        "query": topic
                    }
                }
            })
    print('Get paper by topic query: ', query)
    result = es.search(index=index, body=query)
    print('Get paper by topic: ', result)
    return result['hits']['hits']


def get_all_topics(es, index):
    query = {
        "size": 0,
        "aggs": {
            "topics": {
                "terms": {
                    "field": "topics.topic.keyword"
                }
            }
        }
    }

    result = es.search(index=index, body=query)
    print('Get all topics: ', result)
    return result['aggregations']['topics']


if __name__ == "__main__":
    # get_all_papers(elasticsearch_connection, PAPER_DOCUMENT_INDEX, 0, 10)
    # get_paper_by_topic(elasticsearch_connection, PAPER_DOCUMENT_INDEX, 'Simulation')
    get_paper_by_topic(es=elasticsearch_connection, index=PAPER_DOCUMENT_INDEX, topics=["Engineering"], is_should=False)
