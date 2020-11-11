from elasticsearch import NotFoundError
from es_service.es_constant.constants import HEADERS, PROXY
from collections import Counter
import httpx

PROXIES = {
    'http': PROXY
}


async def get_paper_from_id(es, index, paper_id, isInfluential=None, with_citations=False):
    try:
        paper = es.get(index=index, id=paper_id)
        print(f"found {paper_id} on my api")
        return paper
    except NotFoundError:
        try:
            async with httpx.AsyncClient(proxies=PROXIES) as client:
                response = await client.get("https://api.semanticscholar.org/v1/paper/{}".format(paper_id),
                                            headers=HEADERS)
                paper = response.json()

                res = {"paperId": paper["paperId"],
                       "doi": paper["doi"],
                       "title": paper["title"],
                       "abstract": paper["abstract"],
                       "fieldsOfStudy": paper["fieldsOfStudy"],
                       "topics": [{"topicId": p["topicId"], "topic": p["topic"]} for p in
                                  paper["topics"]],
                       "authors": [{"authorId": a["authorId"], "name": a["name"]} for a in
                                   paper["authors"]],
                       "citations_count": len(paper["citations"]),
                       "references_count": len(paper["references"]),
                       "authors_count": len(paper["authors"]),
                       "venue": paper["venue"],
                       "year": paper["year"]}
                if isInfluential is not None:
                    res["isInfluential"] = isInfluential
                if with_citations:
                    res["citations"] = paper["citations"]
                print(f"found {paper_id} on s2 api")
                return res
        except Exception as e:
            print(f"paper {paper_id} failed to get {e}")
            return None


def common_query__builder(start=0, size=10, source=None, sort_by=None,
                          return_top_author=False, top_author_size=10,
                          return_fos_aggs=False,
                          return_venue_aggs=False,
                          return_year_aggs=False,
                          deep_pagination=False, last_paper_id=None):
    if source is None:
        source = get_paper_default_source()

    if sort_by is None:
        sort_by = get_paper_default_sort(sort_by="score")
    else:
        sort_by = get_paper_default_sort(sort_by=sort_by)

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

    if return_year_aggs:
        query["aggs"]["year_count"] = get_paper_aggregation_by_year()

    print("COMMON_QUERY__BUILDER: ", query)
    return query


def search_paper_year__builder(from_year=0, end_year=2020):
    query = {
        "range": {
            "year": {
                "gte": from_year,
                "lte": end_year
            }
        }
    }
    print("SEARCH_PAPER_YEAR__BUILDER: ", query)
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
    print("SEARCH_PAPER_TITLE__BUILDER: ", query)
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
    print("SEARCH_PAPER_ABSTRACT__BUILDER: ", query)
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
                "topics.topicId.keyword": {
                    "query": topic
                }
            }
        })
    print("SEARCH_PAPER_BY_TOPICS__BUILDER: ", query)
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
    print("SEARCH_PAPER_BY_FOS__BUILDER: ", query)
    return query


def search_paper_by_venues__builder(venues, venues_isShould=True):
    if venues_isShould:
        query = {
            "bool": {
                "should": []
            }
        }
        for venue in venues:
            if venue == "Anonymous":
                venue = ""
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
            if venue == "Anonymous":
                venue = ""
            query["bool"]["must"].append({
                "match": {
                    "venue.keyword": {
                        "query": venue
                    }
                }
            })
    print("SEARCH_PAPER_BY_VENUES__BUILDER: ", query)
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
                        "authors.authorId.keyword": {
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
                            "authors.authorId.keyword": {
                                "query": author
                            }
                        }
                    }
                }
            })

    print("SEARCH_BY_AUTHOR__BUILDER: ", query)
    return query


def get_paper_default_source():
    return ["paperId", "doi", "abstract", "authors", "fieldsOfStudy",
            "title", "topics", "citations_count", "references_count", "authors_count",
            "pdf_url", "venue", "year"]


def get_paper_default_sort(sort_by="score"):
    if sort_by == "score":
        return [{"_score": "desc"}, {"paperId.keyword": "asc"}]
    elif sort_by == "year":
        return [{"year": "desc"}]
    elif sort_by == "citations_count":
        return [{"citations_count": "desc"}]
    else:
        return sort_by


def get_paper_aggregation_of_topics_and_year(topics_size=10, year_size=10):
    return {
        "terms": {
            "field": "topics.topic.keyword",
            "size": topics_size
        },
        "aggs": {
            "years": {
                "terms": {
                    "field": "year",
                    "size": year_size
                }
            }
        }
    }


def get_paper_aggregation_of_topics(size=10):
    return {
        "terms": {
            "field": "topics.topicId.keyword",
            "size": size
        }
    }


def get_paper_aggregation_of_fields_of_study(size=10):
    return {
        "terms": {
            "field": "fieldsOfStudy.keyword",
            "size": size
        }
    }


def get_paper_aggregation_of_venues(size=10):
    return {
        "terms": {
            "field": "venue.keyword",
            "size": size
        }
    }


def get_paper_aggregation_by_year(size=10000):
    return {
        "terms": {
            "field": "year",
            "size": size,
            "order": {
                "_key": "asc"
            }
        }
    }


def get_paper_aggregation_of_authors(size):
    return {
        "nested": {
            "path": "authors"
        },
        "aggs": {
            "name": {
                "terms": {
                    "field": "authors.authorId.keyword",
                    "size": size
                },
                "aggs": {
                    "name": {
                        "terms": {
                            "field": "authors.name.keyword"
                        }
                    }
                }
            }
        }
    }


def get_citations_aggregation_by_year(size):
    return {
        "terms": {
            "field": "citations.year",
            "size": size
        }
    }


def get_citations_aggregation_by_year__S2(citations, size):
    cit_counter = Counter(cit for cit in citations if cit is not None)
    cit_aggs = {year: count for year, count in sorted(cit_counter.most_common(size))}

    return cit_aggs


############################ MATH FUNCTION ##################################
def calculate_paper_hindex(citations):
    # sorting in ascending order
    citations.sort()

    # iterating over the list
    for i, cited in enumerate(citations):
        # finding current result
        result = len(citations) - i

        # if result is less than or equal
        # to cited then return result
        if result <= cited:
            return result

    return 0


def sum(items):
    sum = 0
    for i in items:
        sum += i

    return sum
