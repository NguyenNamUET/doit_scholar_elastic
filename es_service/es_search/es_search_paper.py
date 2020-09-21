from elasticsearch import NotFoundError

from es_service.es_helpers.es_connection import elasticsearch_connection

from es_service.es_search.es_search_helpers import get_paper_default_source, get_paper_aggregation_of_authors, \
    get_paper_aggregation_of_fields_of_study, get_paper_default_sort, \
    get_paper_aggregation_of_venues, get_paper_from_id

from es_service.es_constant.constants import HEADERS, PROXY

import asyncio
import requests


##Straight forward functions (no building query by hand)
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
        paper = es.get(index=index, id=paper_id)

        res = {"paperId": paper['_source']["paperId"],
               "doi": paper['_source']["doi"],
               "corpusId": paper['_source']["corpusId"],
               "title": paper['_source']["title"],
               "venue": paper['_source']["venue"],
               "year": paper['_source']["year"],
               "abstract": paper['_source']["abstract"],
               "authors": paper['_source']["authors"],
               "fieldsOfStudy": paper['_source']["fieldsOfStudy"],
               "topics": [{"topic": "test", "topicId": "000001"}],  ######################
               "citationVelocity": paper['_source']["citationVelocity"],
               "citations": [],
               "references": [],
               "citations_length": paper['_source']["citations_count"],
               "references_length": paper['_source']["references_count"]
               }

        citations = await asyncio.gather(
            *(get_paper_from_id(es, index, citation["paperId"], citation["isInfluential"])
              for citation in paper['_source']["citations"][:5]))

        for c in citations:
            if c is not None:
                res["references"].append({"paperId": c["paperId"],
                                          "title": c["title"],
                                          "authors": [{"authorId": a["authorId"], "name": a["name"]} for a in
                                                      c["authors"]],
                                          "isInfluential": c["isInfluential"],
                                          "venue": c["venue"],
                                          "year": c["year"]})

        references = await asyncio.gather(
            *(get_paper_from_id(es, index, reference["paperId"], reference["isInfluential"])
              for reference in paper['_source']["references"][:5]))
        for r in references:
            if r is not None:
                res["references"].append({"paperId": r["paperId"],
                                          "title": r["title"],
                                          "authors": [{"authorId": a["authorId"], "name": a["name"]} for a in
                                                      r["authors"]],
                                          "isInfluential": r["isInfluential"],
                                          "venue": r["venue"],
                                          "year": r["year"]})
        return res

    except NotFoundError:
        print(f"NOT FOUND {paper_id} ON MY API")
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
               "topics": [{"topic": topic["topic"],
                           "topicId": topic["topicId"]}
                          for topic in paper["topics"]],
               "references_count": len(paper["references"]),
               "fieldsOfStudy": paper["fieldsOfStudy"],
               "citations": [{"paperId": citation["paperId"],
                              "title": citation["title"],
                              "authors": citation["authors"],
                              "isInfluential": citation["isInfluential"],
                              "intent": citation["intent"],
                              "venue": citation["venue"],
                              "year": citation["year"]}
                             for citation in paper["citations"][:5]],
               "references": [{"paperId": reference["paperId"],
                               "title": reference["title"],
                               "authors": reference["authors"],
                               "isInfluential": reference["isInfluential"],
                               "intent": reference["intent"],
                               "venue": reference["venue"],
                               "year": reference["year"]}
                              for reference in paper["references"][:5]],
               "authors": [{"authorId": author["authorId"],
                            "name": author["name"]} for author in paper["authors"]]
               }

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
              "fields": [ "title", "abstract"],
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
    common_query = common_query__builder(start=start, size=size, source=source, sort_by=sort_by,
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
    common_query = common_query__builder(source=["title", "citations_count"], sort_by=[{"citations_count": "desc"}],
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


# def get_some_citations(es, index,
#                          paper_id,
#                          start=0, size=5):
#     try:
#         paper = es.get(index=index, id=paper_id)
#         paper_ids = [citation["paperId"] for citation in paper["_source"]["citations"][start:start + size]]
#         print(paper_ids)
#         common_query = common_query__builder(start=start, size=size,
#                                              source=["paperId", "doi", "authors", "fieldsOfStudy", "title", "topics"],
#                                              return_top_author=True, top_author_size=10,
#                                              return_fos_aggs=True,
#                                              return_venue_aggs=True)
#         query = {
#             "query": {
#                 "bool": {
#                     "should": []
#                 }
#             }
#         }
#         for pid in paper_ids:
#             query["query"]["bool"]["should"].append({
#                 "match": {
#                     "paperId.keyword": pid
#                 }
#             })
#
#         query.update(common_query)
#         print('get_some_citations_2 query: ', query)
#         result = es.search(index=index, body=query)
#         # print('get_some_citations_2 result: ', result)
#
#     except NotFoundError:
#         print('paper {} not found'.format(paper_id))
#         return {}
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

if __name__ == "__main__":
    print(asyncio.run(get_some_citations(es=elasticsearch_connection,
                                      index="paper",
                                      paper_id="7f789c9096e178f15512c123373d1a79dc59f035",
                                      )))

