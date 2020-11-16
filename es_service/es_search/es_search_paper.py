from elasticsearch import NotFoundError

from es_service.es_search.es_search_helpers import common_query__builder, \
    get_paper_aggregation_of_fields_of_study, get_paper_aggregation_of_venues, get_citations_aggregation_by_year, \
    get_citations_aggregation_by_year__S2, \
    get_paper_from_id, \
    search_paper_year__builder, search_paper_by_fos__builder, search_paper_title__builder, \
    search_paper_by_topics__builder, \
    search_by_author__builder, search_paper_by_venues__builder, search_paper_abstract__builder, \
    get_paper_aggregation_of_topics_and_year, rebuild_name_id_aggs
from es_service.es_search.es_search_author import count_authors
from es_service.es_constant.constants import HEADERS, PROXY

import asyncio
import requests
import random

PROXIES = {
    'http': PROXY
}


##COUNTING FUNCTIONS
def get_count_stats(es, index, is_papers_count=False, is_authors_count=False, is_fos_count=False, is_topics_count=False):
    res = {"papers_count": None, "authors_count": None,
           "fos_count": None, "topics_count": None}
    if is_papers_count:
        res["papers_count"] = count_papers(es=es, index=index)
    if is_authors_count:
        res["authors_count"] = count_authors(es=es, index=index)
    if is_fos_count:
        res["fos_count"] = count_fields_of_study(es=es, index=index)
    if is_topics_count:
        res["topics_count"] = count_topics(es=es, index=index)

    return res


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


async def get_paper_by_id(es, index, paper_id, cstart=0, csize=5, rstart=0, rsize=5):
    print(f"get_paper_by_id cstart={cstart} csize={csize} rstart={rstart} rsize={rsize}")
    try:
        ############################ IF FOUND PAPER ON MY ELASTICSEARCH ###################################
        paper = es.get(index=index, id=paper_id)["_source"]
        print(paper.keys())
        print(f"FOUND {paper_id} ON MY API")
        res = {"paperId": paper["paperId"],
               "doi": paper["doi"],
               "corpusId": paper["corpusId"],
               "title": paper["title"],
               "abstract": paper["abstract"],
               "venue": paper["venue"],
               "year": paper["year"],
               "pdf_url": paper["pdf_url"],
               "citationVelocity": paper["citationVelocity"],
               "influentialCitationCount": paper["influentialCitationCount"],
               "citations_count": paper["citations_count"],
               "references_count": paper["references_count"],
               "authors_count": paper["authors_count"],
               "authors": paper["authors"],
               "fieldsOfStudy": paper["fieldsOfStudy"],
               "topics": paper["topics"],
               "citations": [],
               "references": [],
               "citations_chart": get_citations_aggregation_by_year__S2(
                   [cit.get("year", 0) for cit in paper["citations"]],
                   size=200)
               }

    except NotFoundError:
        ####################################### IF FOUND ON S2 API ##########################################
        print(f"NOT FOUND {paper_id} ON MY API")
        try:
            response = requests.get("https://api.semanticscholar.org/v1/paper/{}".format(paper_id),
                                    headers=HEADERS, proxies=PROXIES)
            paper = response.json()
            res = {"paperId": paper["paperId"],
                   "doi": paper["doi"],
                   "corpusId": paper["corpusId"],
                   "title": paper["title"],
                   "abstract": paper["abstract"],
                   "venue": paper["venue"],
                   "year": paper["year"],
                   "pdf_url": None,
                   "citationVelocity": paper["citationVelocity"],
                   "influentialCitationCount": paper["influentialCitationCount"],
                   "citations_count": len(paper["citations"]),
                   "references_count": len(paper["references"]),
                   "authors_count": len(paper["authors"]),
                   "authors": [{"authorId": author["authorId"],
                                "name": author["name"]} for author in paper["authors"]],
                   "fieldsOfStudy": paper["fieldsOfStudy"],
                   "topics": [{"topic": topic["topic"],
                               "topicId": topic["topicId"]}
                              for topic in paper["topics"]],
                   "citations": [],
                   "references": [],
                   "citations_chart": get_citations_aggregation_by_year__S2(
                       [cit.get("year", 0) for cit in paper["citations"]],
                       size=200)
                   }
        except Exception as e:
            res, paper = None, None

    ########## Append citations and references to res ##########
    if res is not None and paper is not None:
        citations = await asyncio.gather(
            *(get_paper_from_id(es, index, citation["paperId"], citation["isInfluential"])
              for citation in paper["citations"][cstart:(cstart + csize)]))
        res["citations"] = [c for c in citations if c is not None]

        references = await asyncio.gather(
            *(get_paper_from_id(es, index, reference["paperId"], reference["isInfluential"])
              for reference in paper["references"][rstart:(rstart + rsize)]))
        res["references"] = [r for r in references if r is not None]
        return res

    else:
        return None


def search_by_title(es, index, search_content,
                    venues=None, venues_isShould=False,
                    authors=None, author_isShould=False,
                    fields_of_study=None, fos_isShould=True,
                    start=0, size=10, source=None, sort_by=None,
                    return_fos_aggs=False,
                    return_venue_aggs=False,
                    from_year=None, end_year=None, return_year_aggs=False,
                    deep_pagination=False, last_paper_id=None,
                    return_top_author=False, top_author_size=10):
    common_query = common_query__builder(start=start, size=size, source=source, sort_by=sort_by,
                                         return_top_author=return_top_author, top_author_size=top_author_size,
                                         return_fos_aggs=return_fos_aggs,
                                         return_venue_aggs=return_venue_aggs,
                                         return_year_aggs=return_year_aggs,
                                         deep_pagination=deep_pagination, last_paper_id=last_paper_id)

    if search_content != "":
        title_query = search_paper_title__builder(search_content=search_content)
        query = {"query":
            {"bool":
                {"must": [
                    {
                        "bool": {
                            "should": title_query
                        }
                    }
                ],
                }
            }
        }
    #Empty search for Highlight FOS (all search bar not allowed empty search content)
    else:
        query = {"query":
            {"bool":
                {"must": [
                    {
                        "match_all": {}
                    }
                ],
                }
            }
        }

    if from_year is not None and end_year is not None:
        year_query = search_paper_year__builder(from_year=from_year, end_year=end_year)
        query["query"]["bool"]["must"].append(year_query)

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
    print("SEARCH_BY_TITLE QUERY: ", query)

    result = es.search(index=index, body=query)
    result["aggregations"] = rebuild_name_id_aggs(result["aggregations"])
    #print("SEARCH_BY_TITLE RESULT: ", result)
    if result["hits"]["total"]["value"] == 0:
        return {}

    return result


def search_by_abstract(es, index, search_content,
                       start=0, size=10, source=None, sort_by=None,
                       return_fos_aggs=False,
                       return_year_aggs=False,
                       deep_pagination=False, last_paper_id=None,
                       return_top_author=False, top_author_size=10):
    common_query = common_query__builder(start=start, size=size, source=source, sort_by=sort_by,
                                         return_top_author=return_top_author, top_author_size=top_author_size,
                                         return_fos_aggs=return_fos_aggs,
                                         return_year_aggs=return_year_aggs,
                                         deep_pagination=deep_pagination, last_paper_id=last_paper_id)
    abstract_query = search_paper_abstract__builder(search_content=search_content)
    query = {"query": abstract_query}

    query.update(common_query)
    print("SEARCH_BY_ABSTRACT QUERY: ", query)

    result = es.search(index=index, body=query)
    result["aggregations"] = rebuild_name_id_aggs(result["aggregations"])
    print("SEARCH_BY_ABSTRACT RESULT: ", result)
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

    print("SEARCH_BY_FIELDS_OF_STUDY QUERY: ", query)

    result = es.search(index=index, body=query)
    result["aggregations"] = rebuild_name_id_aggs(result["aggregations"])
    print("SEARCH_BY_FIELDS_OF_STUDY RESULT: ", result)
    if result["hits"]["total"]["value"] == 0:
        return {}

    return result["hits"]["hits"]


def search_by_venue(es, index,
                    venue=None, search_content=None,
                    authors=None, author_isShould=True, return_top_author=False, top_author_size=10,
                    fields_of_study=None, fos_isShould=True, return_fos_aggs=False,
                    venues=None, venues_isShould=True, return_venue_aggs=False,
                    from_year=None, end_year=None, return_year_aggs=False,
                    start=0, size=10, source=None, sort_by=None):
    common_query = common_query__builder(start=start, size=size,
                                         source=source, sort_by=sort_by,
                                         return_top_author=return_top_author, top_author_size=top_author_size,
                                         return_fos_aggs=return_fos_aggs,
                                         return_venue_aggs=return_venue_aggs,
                                         return_year_aggs=return_year_aggs)
    venue_query = search_paper_by_venues__builder(venues=venue)
    query = {
        "query": {
            "bool": {
                "must": [venue_query]
            }
        }
    }

    if search_content is not None:
        title_query = {"bool":
                           {"should":
                                search_paper_title__builder(search_content)
                            }
                       }
        query["query"]["bool"]["must"].append(title_query)

    if from_year is not None and end_year is not None:
        year_query = search_paper_year__builder(from_year=from_year, end_year=end_year)
        query["query"]["bool"]["must"].append(year_query)

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

    print("SEARCH_BY_VENUE QUERY: ", query)

    result = es.search(index=index, body=query)
    result["aggregations"] = rebuild_name_id_aggs(result["aggregations"])
    result["aggregations"] = rebuild_name_id_aggs(result["aggregations"])
    print("SEARCH_BY_VENUE RESULT: ", result)
    if result["hits"]["total"]["value"] == 0:
        return {}

    return result


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
    print("SEARCH_BY_TOPICS QUERY: ", query)

    result = es.search(index=index, body=query)
    result["aggregations"] = rebuild_name_id_aggs(result["aggregations"])
    print("SEARCH_BY_TOPICS RESULT: ", result)
    if result["hits"]["total"]["value"] == 0:
        return {}

    return result


def search_on_typing(es, index, search_content,
                     authors=None,
                     venues=None,
                     size=10):
    common_query = common_query__builder(source=["paperId", "title", "citations_count"],
                                         sort_by=[{"citations_count": "desc"}],
                                         size=size)
    title_query = search_paper_title__builder(search_content=search_content)
    query = {"query":
                {"bool":
                    {"must": [
                        {"bool": {"should": title_query}}
                    ]
                    }
                }
    }
    query.update(common_query)
    if authors is not None:
        query["query"]["bool"]["must"].append(
            {
                "query": {
                    "match": {
                        "authors.authorId.keyword": authors[0]  # this is id of author
                    }
                }
            })

    elif venues is not None:
        query["query"]["bool"]["must"].append(
            {
                "match": {
                    "venue.keyword": {
                        "query": venues[0]
                    }
                }
            })
    print("SEARCH_ON_TYPING QUERY: ", query)
    result = es.search(index=index, body=query)
    if result["hits"]["total"]["value"] == 0:
        return {}

    print("SEARCH_ON_TYPING RESULT: ", result["hits"]["hits"])
    return result["hits"]["hits"]


async def get_some_citations(es, index, paper_id, start=5, size=5):
    result = []
    try:
        paper = es.get(index=index, id=paper_id)
        print(f"FOUND {paper_id} ON ELAS")
        citations = await asyncio.gather(
            *(get_paper_from_id(es, index, citation["paperId"], isInfluential=citation["isInfluential"])
              for citation in paper['_source']["citations"][start:(start + size)]))

        for c in citations:
            if c is not None:
                result.append(c)

    except NotFoundError:
        response = requests.get("https://api.semanticscholar.org/v1/paper/{}".format(paper_id),
                                headers=HEADERS, proxies=PROXIES)
        paper = response.json()
        print(f"FOUND {paper_id} ON S2 API")
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
        print(f"FOUND {paper_id} ON ELAS")
        references = await asyncio.gather(
            *(get_paper_from_id(es, index, reference["paperId"], isInfluential=reference["isInfluential"])
              for reference in paper['_source']["references"][start:(start + size)]))

        for r in references:
            if r is not None:
                result.append(r)

    except NotFoundError:
        response = requests.get("https://api.semanticscholar.org/v1/paper/{}".format(paper_id),
                                headers=HEADERS, proxies=PROXIES)
        paper = response.json()
        print(f"FOUND {paper_id} ON S2 API")
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
def get_some_papers_for_homepage(es, index, size=10,
                                 order_by_citations_count=False,
                                 order_by_year=False,
                                 order_by_topics__year=None, topics_size=10):
    res = {"most_cited_papers": None,
           "most_recent_paper": None,
           "papers_with_most_popular_topics": None}
    if order_by_citations_count:
        query = {
            "from": random.randint(0, 100),
            "size": size,
            "_source": ["paperId", "title", "abstract", "citations_count", "authors"],
            "sort": [{"citations_count": {"order": "desc"}}]
        }
        print("GET_SOME_PAPER_FOR_HOMEPAGE QUERY: ", query)
        result = es.search(index=index, body=query)
        if result["hits"]["total"]["value"] == 0:
            return {}

        print("GET_SOME_PAPER_FOR_HOMEPAGE RESULT: ", result["hits"]["hits"])
        res["most_cited_papers"] = result["hits"]["hits"]

    if order_by_year:
        query = {
            "from": random.randint(0, 100),
            "size": size,
            "_source": ["paperId", "title", "abstract", "citations_count", "authors"],
            "sort": [{"year": {"order": "desc"}}]
        }
        print("GET_SOME_PAPER_FOR_HOMEPAGE QUERY: ", query)
        result = es.search(index=index, body=query)
        if result["hits"]["total"]["value"] == 0:
            return {}

        print("GET_SOME_PAPER_FOR_HOMEPAGE RESULT: ", result["hits"]["hits"])
        res["most_recent_paper"] = result["hits"]["hits"]

    ###################### PAPER WITH MOST POPULAR TOPICS ##################
    if order_by_topics__year is not None:
        topic_query = {
            "size": 0,
            "aggs": {
                "best_topics_this_year": {
                    "filter": {
                        "term":
                            {"year": order_by_topics__year}
                    },
                    "aggs": {
                        "topics": {
                            "terms": {
                                "field": "topics.topicId.keyword",
                                "size": topics_size
                            }
                        }
                    }
                }
            }
        }

        topic_res = es.search(index=index, body=topic_query)
        topic_list = [topic["key"] for topic in topic_res["aggregations"]["best_topics_this_year"]["topics"]["buckets"]]

        common_query = common_query__builder(source=None,
                                             start=random.randint(0, 100),
                                             size=size)
        query = {"query": search_paper_by_topics__builder(topic_list)}
        query.update(common_query)
        print(query)
        result = es.search(index=index, body=query)
        if result["hits"]["total"]["value"] == 0:
            return {}

        print("GET_SOME_PAPER_FOR_HOMEPAGE RESULT: ", result["hits"]["hits"])
        res["papers_with_most_popular_topics"] = result["hits"]["hits"]

    return res

######################################### GENERATE GRAPHS ####################################
def generate_graphs(es, index,
                    citations_graph=False, paper_id=None, citations_year_range=200,
                    fos_graph=False, venues_graph=False, size=10,
                    topics_graph=False, topics_size=10, year_size=10):
    res = {"fos_chart": None, "venues_chart": None, "topics_chart": None, "citations_chart": None}
    if citations_graph:
        cit_graph = generate_citations_graph(es=es, index=index,
                                             paper_id=paper_id, citations_year_range=citations_year_range)
        if "citations_chart" in cit_graph.keys():
            res["citations_chart"] = cit_graph
    if fos_graph:
        res["fos_chart"] = generate_FOS_graph(es=es, index=index, size=size)
    if venues_graph:
        res["venues_chart"] = generate_venues_graph(es=es, index=index, size=size)
    if topics_graph:
        res["topics_chart"] = generate_topics_graph(es=es, index=index, topics_size=topics_size, year_size=year_size)

    return res


def generate_FOS_graph(es, index, size=10):
    query = {
        "size": 0,
        "aggs": {
            "fos_count": get_paper_aggregation_of_fields_of_study(size=size)
        }
    }
    top_fos = es.search(index=index, body=query)["aggregations"]["fos_count"]["buckets"]
    print("GENERATE_FOS_DONUT_GRAPH RESULT: ", top_fos)
    return {fos["key"]: fos["doc_count"] for fos in top_fos}


def generate_venues_graph(es, index, size=1000):
    query = {
        "size": 0,
        "aggs": {
            "venue_count": get_paper_aggregation_of_venues(size=size)
        }
    }
    top_venues = es.search(index=index, body=query)["aggregations"]["venue_count"]["buckets"]
    print("GENERATE_VENUES_GRAPH RESULT: ", top_venues)
    return {venue["key"]: venue["doc_count"] for venue in top_venues if
            venue["key"] != "" and venue["doc_count"] >= 100}


def generate_topics_graph(es, index, topics_size=10, year_size=10):
    query = {
        "size": 0,
        "aggs": {
            "topic_count": get_paper_aggregation_of_topics_and_year(topics_size=topics_size, year_size=year_size)
        }
    }
    top_topics = es.search(index=index, body=query)["aggregations"]["topic_count"]["buckets"]
    #format: {key: [name,count]}
    res = [{"id": topic["key"].split("___")[1],
             "name": topic["key"].split("___")[0],
             "years_count": topic["years"]["buckets"]}
            for topic in top_topics if topic["key"] != "" and topic["doc_count"] >= 100]
    print("GENERATE_TOPICS_GRAPH RESULT: ", res)
    return res


def generate_citations_graph(es, index, paper_id, citations_year_range=200):
    query = {
        "query": {
            "match": {
                "paperId.keyword": paper_id
            }
        },
        "_source": ["citations.year"],
        "aggs": {
            "citation_year_count": get_citations_aggregation_by_year(size=citations_year_range)
        }
    }
    print("generate_citations_graph", query)
    query_res = es.search(index=index, body=query)

    # IF FOUND PAPER ON MY ELASTICSEARCH
    if query_res['hits']['total']['value'] > 0 and "citations" in query_res["hits"]["hits"][0]["_source"].keys():
        print(f"FOUND {paper_id} ON MY API")

        # BUG ELASTIC AGGS TERM
        # res = {"citations_chart": {bucket['key']:bucket['doc_count'] for bucket in query_res["aggregations"]["citation_year_count"]["buckets"]}}

        res = {"citations_chart": get_citations_aggregation_by_year__S2(
                query_res["hits"]["hits"][0]["_source"]["citations"],
                size=citations_year_range)}
        return res

    # IF FOUND ON S2 API
    else:
        try:
            print(f"NOT FOUND {paper_id} ON MY API")
            response = requests.get("https://api.semanticscholar.org/v1/paper/{}".format(paper_id),
                                    headers=HEADERS, proxies=PROXIES)
            paper = response.json()
            res = {"citations_chart": get_citations_aggregation_by_year__S2([cit["year"] for cit in paper["citations"]],
                                                                            size=citations_year_range)}
            return res
        except Exception as e:
            print(f"ERROR NOT FOUND {paper_id} ON MY API caused by", e)
            return {}


if __name__ == '__main__':
    from es_service.es_helpers.es_connection import elasticsearch_connection
    print(generate_topics_graph(elasticsearch_connection, "paper"))