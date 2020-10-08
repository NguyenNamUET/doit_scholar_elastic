from elasticsearch import NotFoundError

from es_service.es_search.es_search_helpers import get_paper_default_source, get_paper_default_sort, \
    get_paper_aggregation_of_fields_of_study, get_paper_aggregation_of_authors, \
    get_paper_aggregation_of_venues, get_citations_aggregation_by_year, get_citations_aggregation_by_year__S2, \
    get_paper_from_id


from es_service.es_constant.constants import HEADERS, PROXY

import asyncio
import requests
import random


##COUNTING FUNCTIONS
def count_papers(es, index):
    query = {
        "query": {
            "match_all": {}
        },

    }
    res = es.count(body=query, index=index)
    return res["count"]


def count_fields_of_study(es, index):
    query = {
        "size": 0,
        "aggs": {
            "fos_count": {
                "value_count": {
                    "field": "fieldsOfStudy.keyword"
                }
            }
        }
    }

    result = es.search(index=index, body=query)
    print("Get all fields of study result :", result)
    return result["aggregations"]["fos_count"]["value"]


def count_topics(es, index):
    query = {
        "size": 0,
        "aggs": {
            "topics": {
                "value_count": {
                    "field": "topics.topicId.keyword"
                }
            }
        }
    }
    result = es.search(index=index, body=query)
    print('Get all topics result: ', result)
    return result['aggregations']['topics']["value"]


async def get_paper_by_id(es, index, paper_id):
    try:
        ############################ IF FOUND PAPER ON MY ELASTICSEARCH ###################################
        paper = es.get(index=index, id=paper_id)["_source"]
        print(f"FOUND {paper_id} ON MY API")
        res = {"paperId": paper["paperId"],
               "doi": paper["doi"],
               "corpusId": paper["corpusId"],
               "title": paper["title"],
               "venue": paper["venue"],
               "year": paper["year"],
               "abstract": paper["abstract"],
               "citations_count": paper["citations_count"],
               "references_count": paper["references_count"],
               "authors_count": paper["authors_count"],
               "authors": paper["authors"],
               "fieldsOfStudy": paper["fieldsOfStudy"],
               "topics": paper["topics"],
               "influentialCitationCount": paper["influentialCitationCount"],
               "citationVelocity": paper["citationVelocity"],
               "citations": [],
               "references": []
               }

    except NotFoundError:
        ####################################### IF FOUND ON S2 API ##########################################
        print(f"NOT FOUND {paper_id} ON MY API")
        try:
            response = requests.get("https://api.semanticscholar.org/v1/paper/{}".format(paper_id),
                                    headers=HEADERS, proxies=PROXY)
            paper = response.json()
            res = {"paperId": paper["paperId"],
                   "corpusId": paper["corpusId"],
                   "title": paper["title"],
                   "abstract": paper["abstract"],
                   "venue": paper["venue"],
                   "year": paper["year"],
                   "citationVelocity": paper["citationVelocity"],
                   "doi": paper["doi"],
                   "influentialCitationCount": paper["influentialCitationCount"],
                   "citations_count": len(paper["citations"]),
                   "references_count": len(paper["references"]),
                   "authors_count": len(paper["authors"]),
                   "fieldsOfStudy": paper["fieldsOfStudy"],
                   "topics": [{"topic": topic["topic"],
                               "topicId": topic["topicId"]}
                              for topic in paper["topics"]],
                   "citations": [],
                   "references": [],
                   "authors": [{"authorId": author["authorId"],
                                "name": author["name"]} for author in paper["authors"]]
                   }
        except Exception as e:
            res, paper = None, None

    ########## Append citations and references to res ##########
    if res is not None and paper is not None:
        citations = await asyncio.gather(
            *(get_paper_from_id(es, index, citation["paperId"], citation["isInfluential"])
              for citation in paper["citations"][:5]))
        res["citations"] = [c for c in citations if c is not None]

        references = await asyncio.gather(
            *(get_paper_from_id(es, index, reference["paperId"], reference["isInfluential"])
              for reference in paper["references"][:5]))
        res["references"] = [r for r in references if r is not None]


        return res

    else:
        return None


def generate_citations_graph(es, index, paper_id, citations_year_range=200):
    query = {
        "query": {
            "match": {
                "paperId.keyword": paper_id
            }
        },
        "_source": ["citations"],
        "aggs": {
            "citation_year_count": get_citations_aggregation_by_year(size=citations_year_range)
        }
    }
    query_res = es.search(index=index, body=query)

    # IF FOUND PAPER ON MY ELASTICSEARCH
    if query_res['hits']['total']['value'] > 0:
        print(f"FOUND {paper_id} ON MY API")

        # res = {"citations_chart": {bucket['key']:bucket['doc_count'] for bucket in query_res["aggregations"]["citation_year_count"]["buckets"]}}
        res = {"citations_chart": get_citations_aggregation_by_year__S2(
            query_res["hits"]["hits"][0]["_source"]["citations"],
            size=citations_year_range)}
        return res

    # IF FOUND ON S2 API
    else:
        print(f"NOT FOUND {paper_id} ON MY API")
        response = requests.get("https://api.semanticscholar.org/v1/paper/{}".format(paper_id),
                                headers=HEADERS, proxies=PROXY)
        paper = response.json()
        res = {"citations_chart": get_citations_aggregation_by_year__S2(paper["citations"],
                                                                        size=citations_year_range)}

        return res


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
    query = [
        {
            "multi_match": {
                "query": search_content,
                "fields": ["title", "abstract"],
                "operator": "and",
                "boost": 2
            }
        },
        {
            "match": {
                "abstract": {
                    "query": search_content,
                    "operator": "and"
                }
            }
        },
        {
            "match": {
                "title": {
                    "query": search_content,
                    "operator": "and"
                }
            }
        }
    ]
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
    common_query = common_query__builder(start=start, size=size, source=source,
                                         sort_by=[{"_score": "desc"}, {"citations_count": "desc"},
                                                  {"references_count": "desc"}],
                                         return_top_author=return_top_author, top_author_size=top_author_size,
                                         return_fos_aggs=return_fos_aggs,
                                         return_venue_aggs=return_venue_aggs,
                                         deep_pagination=deep_pagination, last_paper_id=last_paper_id)

    title_query = search_paper_title__builder(search_content=search_content)
    query = {"query":
                 {"bool":
                      {"must": [],
                       "should": title_query}
                  }
             }

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

    return result["hits"]["hits"]


def search_by_venue(es, index,
                    venue=None,
                    start=0, size=10, source=None, sort_by=None):
    common_query = common_query__builder(start=start, size=size, source=source, sort_by=sort_by
                                         )
    venue_query = search_paper_by_venues__builder(venues=venue)
    query = {"query": venue_query}
    query.update(common_query)

    print("search_by_venue query: ", query)

    result = es.search(index=index, body=query)

    print("search_by_venue result: ", result)
    if result["hits"]["total"]["value"] == 0:
        return {}

    return result["hits"]["hits"]


def search_by_topics(es, index,
                     topics=None, topic_isShould=True,
                     start=0, size=10, source=None, sort_by=None,
                     return_fos_aggs=False,
                     deep_pagination=False, last_paper_id=None,
                     return_top_author=False, top_author_size=10,
                     ):
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
    common_query = common_query__builder(source=["title", "citations_count", "year"],
                                         sort_by=[{"citations_count": "desc"}],
                                         size=size)
    title_query = search_paper_title__builder(search_content=search_content)
    query = {"query":
                 {"bool":
                      {"must": [],
                       "should": title_query}
                  }
             }
    query.update(common_query)
    print("search_on_typing query: ", query)
    result = es.search(index=index, body=query)
    if result["hits"]["total"]["value"] == 0:
        return {}

    print("search_on_typing result: ", result["hits"]["hits"])
    return result["hits"]["hits"]


async def get_some_citations(es, index, paper_id, start=5, size=5):
    result = []
    try:
        paper = es.get(index=index, id=paper_id)

        citations = await asyncio.gather(
            *(get_paper_from_id(es, index, citation["paperId"], citation["isInfluential"])
              for citation in paper['_source']["citations"][start:(start + size)]))

        for c in citations:
            if c is not None:
                result.append({"paperId": c["paperId"],
                               "title": c["title"],
                               "authors": [{"authorId": a["authorId"], "name": a["name"]} for a in
                                           c["authors"]],
                               "isInfluential": c["isInfluential"],
                               "venue": c["venue"],
                               "year": c["year"]})

    except NotFoundError:
        response = requests.get("https://api.semanticscholar.org/v1/paper/{}".format(paper_id),
                                headers=HEADERS, proxies=PROXY)
        paper = response.json()
        result = [{"paperId": citation["paperId"],
                   "title": citation["title"],
                   "authors": citation["authors"],
                   "isInfluential": citation["isInfluential"],
                   "intent": citation["intent"],
                   "venue": citation["venue"],
                   "year": citation["year"]}
                  for citation in paper["citations"][start:(start + size)]]
    return result


async def get_some_references(es, index, paper_id, start=5, size=5):
    result = []
    try:
        paper = es.get(index=index, id=paper_id)

        references = await asyncio.gather(
            *(get_paper_from_id(es, index, reference["paperId"], reference["isInfluential"])
              for reference in paper['_source']["references"][start:(start + size)]))

        for c in references:
            if c is not None:
                result.append({"paperId": c["paperId"],
                               "title": c["title"],
                               "authors": [{"authorId": a["authorId"], "name": a["name"]} for a in
                                           c["authors"]],
                               "isInfluential": c["isInfluential"],
                               "venue": c["venue"],
                               "year": c["year"]})

    except NotFoundError:
        response = requests.get("https://api.semanticscholar.org/v1/paper/{}".format(paper_id),
                                headers=HEADERS, proxies=PROXY)
        paper = response.json()
        result = [{"paperId": reference["paperId"],
                   "title": reference["title"],
                   "authors": reference["authors"],
                   "isInfluential": reference["isInfluential"],
                   "intent": reference["intent"],
                   "venue": reference["venue"],
                   "year": reference["year"]}
                  for reference in paper["references"][start:(start + size)]]
    return result


##################################### HOMEPAGE FUNCTION ###############################################
def get_some_papers_for_homepage(es, index, size=3):
    query = {
        "query": {
            "match_all": {}
        },
        "from": random.randint(0, 100),
        "size": size,
        "_source": ["paperId", "title", "abstract", "citations_count", "authors"],
        "sort": [{"citations_count": {"order": "desc"}}]
    }
    print("get_some_papers_for_homepage query: ", query)
    result = es.search(index=index, body=query)

    if result["hits"]["total"]["value"] == 0:
        return {}

    print("get_some_papers_for_homepage result: ", result["hits"]["hits"])
    return result["hits"]["hits"]


def generate_FOS_donut_graph(es, index, size=10):
    query = {
        "query": {
            "match_all": {}
        },
        "size": 0,
        "aggs": {
            "fos_aggs": get_paper_aggregation_of_fields_of_study(size=size)
        }
    }
    top_fos = es.search(index=index, body=query)["aggregations"]["fos_aggs"]["buckets"]
    print("generate_FOS_donut_graph__homepage result: ", top_fos)
    return {fos["key"]: fos["doc_count"] for fos in top_fos}


def generate_venues_graph(es, index, size=1000):
    query = {
        "query": {
            "match_all": {}
        },
        "size": 0,
        "aggs": {
            "venue_aggs": get_paper_aggregation_of_venues(size=size)
        }
    }
    top_venues = es.search(index=index, body=query)["aggregations"]["venue_aggs"]["buckets"]
    print("generate_venue_graph result: ", top_venues)
    return {venue["key"]: venue["doc_count"] for venue in top_venues if venue["key"]!="" and venue["doc_count"] >= 100}
