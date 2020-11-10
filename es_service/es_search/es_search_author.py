from es_service.es_constant.constants import HEADERS, PROXY
from es_service.es_search.es_search_helpers import common_query__builder, \
    get_paper_aggregation_of_authors, \
    search_paper_year__builder, search_paper_by_fos__builder, search_by_author__builder, \
    search_paper_by_venues__builder, search_paper_title__builder, \
    calculate_paper_hindex, get_paper_from_id, sum, get_citations_aggregation_by_year, \
    get_citations_aggregation_by_year__S2

import requests
import asyncio

PROXIES = {
    'http': PROXY
}


def count_authors(es, index):
    query = {
        "size": 0,
        "aggs": {
            "author_aggs": {
                "nested": {
                    "path": "authors"
                },
                "aggs": {
                    "author_count": {
                        "value_count": {
                            "field": "authors.authorId.keyword"
                        }
                    }
                }
            }
        }
    }
    res = es.search(index=index, body=query)
    print("Get all authors result :", res)
    return res["aggregations"]["author_aggs"]["author_count"]["value"]


def get_some_papers(es, index, author_id, start=5, size=5):
    query = {
        "query": {
            "nested": {
                "path": "authors",
                "query": {
                    "match": {
                        "authors.authorId.keyword": author_id  # 150080110
                    }
                }
            }
        },
        "from": start,
        "size": size,
        "_source": ["paperId", "doi", "abstract", "authors", "fieldsOfStudy",
                    "title", "topics", "citations_count", "references_count", "authors_count",
                    "pdf_url", "venue", "year"],
    }

    res = es.search(index=index, body=query)
    if res["hits"]["total"]["value"] > 0:
        print(f"FOUND AUTHOR {author_id} ON ELASTIC")
        print("get_some_papers result: ", res)
        return [p["_source"] for p in res["hits"]["hits"]]
    else:
        print(f"NOT FOUND AUTHOR {author_id} ON MY API")
        response = requests.get("https://api.semanticscholar.org/v1/author/{}".format(author_id),
                                headers=HEADERS, proxies=PROXIES)
        json_res = response.json()
        return [{"paperId": paper["paperId"],
                 "title": paper["title"],
                 "year": paper["year"]} for paper in json_res["papers"][start:(start + size)]]


async def get_author_by_id(es, index, author_id, start=0, size=5,
                           sort_by=None, search_content=None,
                           authors=None, author_isShould=True, return_top_author=False, top_author_size=10,
                           fields_of_study=None, fos_isShould=True, return_fos_aggs=False,
                           venues=None, venues_isShould=True, return_venue_aggs=False,
                           from_year=None, end_year=None, return_year_aggs=False):
    common_query = common_query__builder(start=start, size=size,
                                         source=["paperId", "doi", "abstract", "authors", "fieldsOfStudy", "citations",
                                                 "title", "topics", "citations_count", "references_count",
                                                 "authors_count", "pdf_url", "venue", "year"],
                                         sort_by=sort_by,
                                         return_top_author=return_top_author, top_author_size=top_author_size,
                                         return_fos_aggs=return_fos_aggs,
                                         return_venue_aggs=return_venue_aggs,
                                         return_year_aggs=return_year_aggs)
    common_query["aggs"]["influentialCitationCount"] = {
        "sum": {
            "field": "influentialCitationCount"
        }
    }
    common_query["aggs"]["citationsCount"] = {
        "sum": {
            "field": "citations_count"
        }
    }

    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "nested": {
                            "path": "authors",
                            "query": {
                                "match": {
                                    "authors.authorId.keyword": author_id
                                }
                            }
                        }
                    }
                ]
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
    print("GET_AUTHOR_BY_ID query", query)

    res = es.search(index=index, body=query)

    if res["hits"]["total"]["value"] > 0:
        print(f"FOUND AUTHOR {author_id} ON ELASTIC")
        year_list = []
        for paper in res["hits"]["hits"]:
            for cit in paper["_source"]["citations"]:
                year_list.append(cit.get("year", 0))
        print(year_list)
        res["aggregations"]["citations_chart"] = get_citations_aggregation_by_year__S2(year_list, size=200)
        #Remove citaions key from paper
        res["hits"]["hits"] = [{k: v for k, v in p.items() if k != "citations"} for p in res["hits"]["hits"]]
        res["aggregations"]["totalPapers"] = res["hits"]["total"]["value"]
        res["authorId"] = author_id
        res["name"] = [p["name"] for p in res["hits"]["hits"][0]["_source"]["authors"] if p["authorId"] == author_id][0]
        res["h_index"] = calculate_paper_hindex([p["_source"]["citations_count"] for p in res["hits"]["hits"]])

        return res
    else:
        print(f"NOT FOUND AUTHOR {author_id} ON MY API")
        response = requests.get("https://api.semanticscholar.org/v1/author/{}".format(author_id),
                                headers=HEADERS, proxies=PROXIES)
        json_res = response.json()

        papers = await asyncio.gather(
            *(get_paper_from_id(es, index, p["paperId"], with_citations=True)
              for p in json_res["papers"]))
        res = {
                "hits": {
                    "hits": [{"paperId": paper["paperId"],
                            "title": paper["title"],
                            "year": paper["year"]} for paper in json_res["papers"][:size] if paper is not None]
                },
                "aggregations": {
                    "influentialCitationCount": {"value": json_res["influentialCitationCount"]},
                    "totalPapers": {"value": len(json_res["papers"])},
                    "citationsCount": {"value": sum([len(p["citations"]) for p in papers])},
                    "citations_chart": get_citations_aggregation_by_year__S2([cit.get("year",0) for cit in papers["citations"]],
                                                                             size=200)
                }
            }
        res["authorId"] = author_id
        res["name"] = json_res["name"]
        res["h_index"] = calculate_paper_hindex([len(p["citations"]) for p in papers])

        return res


##################################### HOMEPAGE FUNCTION ###############################################
async def get_some_authors_for_homepage(es, index, size=3):
    p_query = {
        "query": {
            "match_all": {}
        },
        "size": 1000,
        "_source": ["paperId"],
        "sort": [{"citations_count": {"order": "desc"}}]
    }
    top_referenced_papers = es.search(index=index, body=p_query)

    query = {
        "query": {
            "terms": {
                "paperId.keyword": [paper["_source"]["paperId"] for paper in top_referenced_papers["hits"]["hits"]]
            }
        },
        "size": 0,
        "aggs": {
            "authors_agg": get_paper_aggregation_of_authors(size)
        }
    }
    res = es.search(index=index, body=query)

    top_referenced_authors = await asyncio.gather(
        *(get_author_by_id(es, index, author["key"])
          for author in res["aggregations"]["authors_agg"]["name"]["buckets"]))

    print("get_some_authors_for_homepage result: ", top_referenced_authors)
    return top_referenced_authors
