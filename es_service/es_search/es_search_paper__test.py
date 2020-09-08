from elasticsearch import NotFoundError

from es_constant.constants import PAPER_DOCUMENT_INDEX
from es_service.es_helpers.es_connection import elasticsearch_connection

from es_service.es_search.es_search_helpers import get_paper_default_source, get_paper_aggregation_of_authors, \
    get_paper_aggregation_of_fields_of_study, get_paper_default_sort, count_fields_of_study_buckets, \
    get_paper_aggregation_of_venues


##Straight forward functions (no building query by hand)
def get_paper_by_id(es, index, paper_id):
    try:
        paper = es.get(index=index, id=paper_id)
        res = {"doi": paper['_source']["doi"],
               "corpusId": paper['_source']["corpusId"],
               "title": paper['_source']["title"],
               "venue": paper['_source']["venue"],
               "year": paper['_source']["year"],
               "abstract": paper['_source']["abstract"],
               "authors": paper['_source']["authors"],
               "fieldsOfStudy": paper['_source']["fieldsOfStudy"],
               "topics": paper['_source']["topics"],
               "citationVelocity": paper['_source']["citationVelocity"],
               "citations": paper['_source']["citations"][:5],
               "references": paper['_source']["references"][:5],
               "citations_length": len(paper['_source']["citations"]),
               "references_length": len(paper['_source']["references"])
               }
        return res
    except NotFoundError:
        print('not found')
        return {}


# These builder function only return part of query
# We will assemble them later
def common_query__builder(start=0, size=10, source=None, sort_by=None,
                          return_top_author=False, top_author_size=10,
                          return_fos_aggs=False,
                          return_venue_aggs=False,
                          deep_pagination=False, last_paper_id=None):
    if source is None:
        source = get_paper_default_source()

    if sort_by is None:
        sort_by = get_paper_default_sort()

    query = {"from": start,
             "size": size,
             "aggs": {},
             "_source": source,
             "sort": sort_by}

    if deep_pagination:
        query["search_after"] = [last_paper_id, 0]
        query["from"] = 0

    if return_top_author:
        query["aggs"]["author_count"] = get_paper_aggregation_of_authors(size=top_author_size)

    if return_fos_aggs:
        query["aggs"]["fos_count"] = get_paper_aggregation_of_fields_of_study()

    if return_venue_aggs:
        query["aggs"]["venue_count"] = get_paper_aggregation_of_venues()

    print("common_query__builder: ", query)
    return query


def search_paper_title__builder(search_content):
    query = {
        "match": {
            "title": {
                "query": search_content,
                "fuzziness": 2
            }
        }
    }
    print("search_paper_title__builder: ", query)
    return query


def search_paper_abstract__builder(search_content):
    query = {
        "match": {
            "abstract": {
                "query": search_content,
                "fuzziness": 1
            }
        }
    }
    print("search_paper_abstract__builder: ", query)
    return query


def search_paper_by_topics__builder(topics, topic_isShould=True):
    query = {
        "bool": {
            "should": []
        }
    }
    for topic in topics:
        query["bool"]["should"].append({
            "match": {
                "topics.topic.keyword": {
                    "query": topic
                }
            }
        })
    print("search_paper_by_topics__builder: ", query)
    return query


def search_paper_by_fos__builder(fields_of_study, fos_isShould=True):
    if fos_isShould:
        query = {
            "bool": {
                "should": []
            }
        }
        for fos in fields_of_study:
            query["bool"]["should"].append({
                "match": {
                    "fieldsOfStudy.keyword": {
                        "query": fos
                    }
                }
            })
    else:
        query = {
            "bool": {
                "must": []
            }
        }
        for fos in fields_of_study:
            query["bool"]["must"].append({
                "match": {
                    "fieldsOfStudy.keyword": {
                        "query": fos
                    }
                }
            })
    print("search_paper_by_fos__builder: ", query)
    return query


def search_paper_by_venues__builder(venues, venues_isShould=True):
    if venues_isShould:
        query = {
            "bool": {
                "should": []
            }
        }
        for venue in venues:
            query["bool"]["should"].append({
                "match": {
                    "venue.keyword": {
                        "query": venue
                    }
                }
            })
    else:
        query = {
            "bool": {
                "must": []
            }
        }
        for venue in venues:
            query["bool"]["must"].append({
                "match": {
                    "venue.keyword": {
                        "query": venue
                    }
                }
            })
    print("search_paper_by_venues__builder: ", query)
    return query


def search_by_author__builder(authors, author_isShould):
    if author_isShould:
        query = {
            "nested": {
                "path": "authors",
                "query": {
                    "bool": {
                        "should": []
                    }
                }
            }
        }
        for author in authors:
            query["nested"]["query"]["bool"]["should"].append(
                {
                    "match": {
                        "authors.name.keyword": {
                            "query": author
                        }
                    }
                }
            )
    else:
        query = {"query": []}
        for author in authors:
            query["query"].append({
                "nested": {
                    "path": "authors",
                    "query": {
                        "match": {
                            "authors.name.keyword": {
                                "query": author
                            }
                        }
                    }
                }
            })

    print("search_by_author__builder: ", query)
    return query


####I assemble these builder here to create function
def search_by_title(es, index, search_content,
                    venues=None, venues_isShould=False,
                    authors=None, author_isShould=False,
                    fields_of_study=None, fos_isShould=True,
                    start=0, size=10, source=None, sort_by=None,
                    return_fos_aggs=False,
                    return_venue_aggs=False,
                    deep_pagination=False, last_paper_id=None,
                    return_top_author=False, top_author_size=10):
    common_query = common_query__builder(start=start, size=size, source=source, sort_by=sort_by,
                                         return_top_author=return_top_author, top_author_size=top_author_size,
                                         return_fos_aggs=return_fos_aggs,
                                         return_venue_aggs=return_venue_aggs,
                                         deep_pagination=deep_pagination, last_paper_id=last_paper_id)
    title_query = search_paper_title__builder(search_content=search_content)
    query = {"query":
                 {"bool":
                      {"must": []}
                  }
             }
    query["query"]["bool"]["must"].append(title_query)

    if venues is not None:
        venues_query = search_paper_by_venues__builder(venues=venues, venues_isShould=venues_isShould)

        query["query"]["bool"]["must"].append(venues_query)

    if fields_of_study is not None:
        fos_query = search_paper_by_fos__builder(fields_of_study=fields_of_study,
                                                 fos_isShould=fos_isShould)

        query["query"]["bool"]["must"].append(fos_query)

    if authors is not None:
        authors_query = search_by_author__builder(authors=authors,
                                                  author_isShould=author_isShould)

        if author_isShould:
            query["query"]["bool"]["must"].append(authors_query)
        else:
            for author_query in authors_query["query"]:
                query["query"]["bool"]["must"].append(author_query)

    query.update(common_query)
    print("search_by_title query: ", query)

    result = es.search(index=index, body=query)
    print("search_by_title result: ", result)
    if result["hits"]["total"]["value"] == 0:
        return {}

    return result


def search_by_abstract(es, index, search_content,
                       start=0, size=10, source=None, sort_by=None,
                       return_fos_aggs=False,
                       deep_pagination=False, last_paper_id=None,
                       return_top_author=False, top_author_size=10):
    common_query = common_query__builder(start=start, size=size, source=source, sort_by=sort_by,
                                         return_top_author=return_top_author, top_author_size=top_author_size,
                                         return_fos_aggs=return_fos_aggs,
                                         deep_pagination=deep_pagination, last_paper_id=last_paper_id)
    abstract_query = search_paper_abstract__builder(search_content=search_content)
    query = {"query": abstract_query}

    query.update(common_query)
    print("search_by_abstract query: ", query)

    result = es.search(index=index, body=query)
    print("search_by_abstract result: ", result)
    if result["hits"]["total"]["value"] == 0:
        return {}

    return result


def search_by_fields_of_study(es, index,
                              fields_of_study=None, fos_isShould=True,
                              start=0, size=10, source=None, sort_by=None,
                              return_fos_aggs=False,
                              deep_pagination=False, last_paper_id=None,
                              return_top_author=False, top_author_size=10):
    common_query = common_query__builder(start=start, size=size, source=source, sort_by=sort_by,
                                         return_top_author=return_top_author, top_author_size=top_author_size,
                                         return_fos_aggs=return_fos_aggs,
                                         deep_pagination=deep_pagination, last_paper_id=last_paper_id)
    fos_query = search_paper_by_fos__builder(fields_of_study=fields_of_study,
                                             fos_isShould=fos_isShould)
    query = {"query": fos_query}
    query.update(common_query)
    print("search_by_fields_of_study query: ", query)

    result = es.search(index=index, body=query)
    print("search_by_fields_of_study result: ", result)
    if result["hits"]["total"]["value"] == 0:
        return {}

    return result.hits.hits


def search_by_topics(es, index,
                     topics=None, topic_isShould=True,
                     start=0, size=10, source=None, sort_by=None,
                     return_fos_aggs=False,
                     deep_pagination=False, last_paper_id=None,
                     return_top_author=False, top_author_size=10):
    common_query = common_query__builder(start=start, size=size, source=source, sort_by=sort_by,
                                         return_top_author=return_top_author, top_author_size=top_author_size,
                                         return_fos_aggs=return_fos_aggs,
                                         deep_pagination=deep_pagination, last_paper_id=last_paper_id)
    topic_query = search_paper_by_topics__builder(topics=topics,
                                                  topic_isShould=topic_isShould)
    query = {"query": topic_query}
    query.update(common_query)
    print("search_by_topics query: ", query)

    result = es.search(index=index, body=query)
    print("search_by_topics result: ", result)
    if result["hits"]["total"]["value"] == 0:
        return {}

    return result


def search_on_typing(es, index, search_content, size=10):
    common_query = common_query__builder(source=["title", "citations_count"], sort_by=[{"citations_count": "desc"}])
    query = {
      "query": {
        "match": {
          "title": {
            "query": search_content
          }
        }
      }
    }
    query.update(common_query)
    print("search_on_typing query: ", query)

    result = es.search(index=index, body=query)
    if result["hits"]["total"]["value"] == 0:
        return {}

    print("search_on_typing result: ", result["hits"]["hits"])
    return result["hits"]["hits"]


# These functions are merely for counting (no complex search)
def get_all_fields_of_study(es, index, size=10):
    query = {
        "size": 0,
        "aggs": {
            "fields_of_study": get_paper_aggregation_of_fields_of_study(),
            "fos_unique_count": count_fields_of_study_buckets()
        }
    }

    print("Get all fields of study query :", query)
    result = es.search(index=index, body=query)
    print("Get all fields of study result :", result)
    return result["aggregations"]


def get_all_papers(es, index,
                   start=0, size=10, source=None, sort_by=None,
                   return_fos_aggs=False,
                   deep_pagination=False, last_paper_id=None,
                   return_top_author=False, top_author_size=10):
    common_query = common_query__builder(start=start, size=size, source=source, sort_by=sort_by,
                                         return_top_author=return_top_author,
                                         top_author_size=top_author_size,
                                         return_fos_aggs=return_fos_aggs,
                                         deep_pagination=deep_pagination, last_paper_id=last_paper_id)
    query = {
        "query": {
            "match_all": {}
        },

    }
    query.update(common_query)
    print("Get all papers query :", query)
    result = es.search(index=index, body=query)
    print("Get all papers result :", result)
    return result["hits"]


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
    print('Get all topics query: ', query)
    result = es.search(index=index, body=query)
    print('Get all topics result: ', result)
    return result['aggregations']['topics']


def get_some_citations(es, index,
                       paper_id,
                       start=0, size=5):
    try:
        res = es.get(index=index, id=paper_id)
        return res["_source"]["citations"][start:start + size]
    except NotFoundError:
        print('paper {} not found'.format(paper_id))
        return {}


def get_some_references(es, index,
                        paper_id,
                        start=0, size=5):
    try:
        res = es.get(index=index, id=paper_id)
        return res["_source"]["references"][start:start + size]
    except NotFoundError:
        print('paper {} not found'.format(paper_id))
        return {}


if __name__ == "__main__":
    search_on_typing(elasticsearch_connection, "paper_test", "Fenomén")
