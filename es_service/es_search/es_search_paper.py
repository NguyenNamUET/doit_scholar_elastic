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


def get_paper_by_id(es, index, paper_id):
    try:
        res = es.get(index=index, id=paper_id)
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


def search_paper_title(search_content, es, index, start=0, size=10, source=None, sort_by=None,
                       return_top_author=False, top_author_size=10,
                       return_fos_aggs=False,
                       deep_pagination=False, last_paper_id=None):
    if source is None:
        source = get_paper_default_source()

    if sort_by is None:
        sort = get_paper_default_sort()

    query = ""
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
                "aggs": {},
                "_source": source,
                "sort": sort
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
            "aggs": {},
            "_source": source,
            "sort": sort
        }

    if deep_pagination:
        query["search_after"] = [last_paper_id, 0]
        query["from"] = 0

    if return_top_author:
        query["aggs"]["author_count"] = get_paper_aggregation_of_authors(size=top_author_size)

    if return_fos_aggs:
        query["aggs"]["fields_of_study"] = get_paper_aggregation_of_fields_of_study()

    print("Query: ", query)
    result = es.search(index=index, body=query)
    print("Search paper title :", result)
    if result["hits"]["total"]["value"] == 0:
        return {}

    return result


def search_paper_abstract(search_content, es, index, start, size, source=None, sort_by=None,
                          return_fos_aggs=False,
                          return_top_author=False, top_author_size=10,
                          deep_pagination=False, last_paper_id=None):
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
            "aggs": {},
            "_source": source,
            "sort": sort
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
            "aggs": {},
            "_source": source,
            "sort": sort
        }

    if deep_pagination:
        query["search_after"] = [last_paper_id, 0]
        query["from"] = 0

    if return_top_author:
        query["aggs"]["author_count"] = get_paper_aggregation_of_authors(size=top_author_size)

    if return_fos_aggs:
        query["aggs"]["fields_of_study"] = get_paper_aggregation_of_fields_of_study()

    result = es.search(index=index, body=query)
    print("Search paper abstract :", result)
    if result["hits"]["total"]["value"] == 0:
        return {}

    return result


def search_paper_by_fos(es, index, fields_of_study,
                        start=0, size=10, source=None, sort_by=None, is_should=True,
                        return_fos_aggs=False,
                        return_top_author=False, top_author_size=10,
                        deep_pagination=False, last_paper_id=None,
                        debug=False):
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
            "aggs": {},
            "_source": source,
            "sort": sort
        }
        for fos in fields_of_study:
            query["query"]["bool"]["should"].append({
                "match": {
                    "fieldsOfStudy.keyword": {
                        "query": fos
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
            "aggs": {},
            "_source": source,
            "sort": sort
        }
        for fos in fields_of_study:
            query["query"]["bool"]["must"].append({
                "match": {
                    "fieldsOfStudy.keyword": {
                        "query": fos
                    }
                }
            })

    if deep_pagination:
        query["search_after"] = [last_paper_id, 0]
        query["from"]=0

    if return_top_author:
        query["aggs"]["author_count"] = get_paper_aggregation_of_authors(size=top_author_size)

    if return_fos_aggs:
        query["aggs"]["fields_of_study"] = get_paper_aggregation_of_fields_of_study()

    if debug:
        return query
    else:
        print('Get paper by topic query: ', query)
        result = es.search(index=index, body=query)
        print('Get paper by topic: ', result)
        if result["hits"]["total"]["value"] == 0:
            return {}

        return result


def search_paper_by_title_and_fos(es, index, search_content, fields_of_study,
                                  start=0, size=10, source=None, sort_by=None,
                                  is_should=True,
                                  return_fos_aggs=False,
                                  deep_pagination=False, last_paper_id=None,
                                  return_top_author=False, top_author_size=10):

    fos_query = search_paper_by_fos(es=es, index=index, fields_of_study=fields_of_study, is_should=is_should,
                                    start=start, size=size, source=source, sort_by=sort_by,
                                    return_fos_aggs=return_fos_aggs,
                                    return_top_author=return_top_author, top_author_size=top_author_size,
                                    deep_pagination=deep_pagination, last_paper_id=last_paper_id,
                                    debug=True)

    query = fos_query
    if is_should:
        query["query"]["bool"]["must"] = []
        query["query"]["bool"]["must"].append({
            "match": {
                "title": {
                    "query": search_content,
                    "fuzziness": 1
                }
            }
        })

    else:
        query["query"]["bool"]["must"].append({
            "match": {
                "title": {
                    "query": search_content,
                    "fuzziness": 1
                }
            }
        })

    result = es.search(index=index, body=query)
    print('Get paper by title and topics query: ', query)
    print('Get paper by title and topics result: ', result)
    if result["hits"]["total"]["value"] == 0:
        return {}

    return result


if __name__ == "__main__":
    # get_all_papers(elasticsearch_connection, PAPER_DOCUMENT_INDEX, 0, 10)

    search_paper_by_title_and_fos(es=elasticsearch_connection, index=PAPER_DOCUMENT_INDEX,
                                  search_content="a",
                                  fields_of_study=['Engineering'],
                                  source=["corpusID"],
                                  size=2,
                                  return_fos_aggs=True,
                                  return_top_author=True, top_author_size=10,
                                  deep_pagination=True, last_paper_id=20496546)
