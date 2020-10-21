from es_service.es_constant.constants import HEADERS, PROXY
from es_service.es_search.es_search_helpers import get_paper_default_sort, get_paper_aggregation_of_authors, \
    calculate_paper_hindex, get_paper_from_id, sum

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


async def get_author_by_id(es, index, author_id, start=0, size=5, sort_by="score",
                           shorted=False):
    query = {
        "query": {
            "nested": {
                "path": "authors",
                "query": {
                    "match": {
                        "authors.authorId.keyword": author_id
                    }
                }
            }
        },
        "from": start,
        "size": size,
        "_source": ["paperId", "doi", "abstract", "authors", "fieldsOfStudy",
                    "title", "topics", "citations_count", "references_count", "authors_count",
                    "pdf_url", "venue", "year"],
        "aggs": {
            "influentialCitationCount": {
                "sum": {
                    "field": "influentialCitationCount"
                }
            },
            "citationsCount": {
                "sum": {
                    "field": "citations_count"
                }
            },
            "totalPapers": {
                "value_count": {
                    "field": "paperId.keyword"
                }
            }
        },
        "sort": get_paper_default_sort(sort_by=sort_by)
    }
    res = es.search(index=index, body=query)

    if res["hits"]["total"]["value"] > 0:
        print(f"FOUND AUTHOR {author_id} ON ELASTIC")
        if shorted:
            author = {
                "authorId": author_id,
                "influentialCitationCount": res["aggregations"]["influentialCitationCount"]["value"],
                "h_index": calculate_paper_hindex([p["_source"]["citations_count"] for p in res["hits"]["hits"]]),
                "totalPapers": res["aggregations"]["totalPapers"]["value"],
                "citationsCount": res["aggregations"]["citationsCount"]["value"],
                "name": [p["name"] for p in res["hits"]["hits"][0]["_source"]["authors"] if p["authorId"] == author_id][
                    0]
            }
        else:
            author = {
                "authorId": author_id,
                "influentialCitationCount": res["aggregations"]["influentialCitationCount"]["value"],
                "totalPapers": res["aggregations"]["totalPapers"]["value"],
                "citationsCount": res["aggregations"]["citationsCount"]["value"],
                "h_index": calculate_paper_hindex([p["_source"]["citations_count"] for p in res["hits"]["hits"]]),
                "name": [p["name"] for p in res["hits"]["hits"][0]["_source"]["authors"] if p["authorId"] == author_id][
                    0],
                "papers": [p["_source"] for p in res["hits"]["hits"]]
            }
        return author
    else:
        print(f"NOT FOUND AUTHOR {author_id} ON MY API")
        response = requests.get("https://api.semanticscholar.org/v1/author/{}".format(author_id),
                                headers=HEADERS, proxies=PROXIES)
        json_res = response.json()

        papers = await asyncio.gather(
            *(get_paper_from_id(es, index, p["paperId"], with_citations=True)
              for p in json_res["papers"]))
        if shorted:
            author = {
                "authorId": author_id,
                "influentialCitationCount": json_res["influentialCitationCount"],
                "citationsCount": sum([len(p["citations"]) for p in papers]),
                "h_index": calculate_paper_hindex([len(p["citations"]) for p in papers]),
                "totalPapers": len(json_res["papers"]),
                "name": json_res["name"]
            }
        else:
            author = {
                "authorId": author_id,
                "influentialCitationCount": json_res["influentialCitationCount"],
                "citationsCount": sum([len(p["citations"]) for p in papers]),
                "h_index": calculate_paper_hindex([len(p["citations"]) for p in papers]),
                "totalPapers": len(json_res["papers"]),
                "name": json_res["name"],
                "papers": [{"paperId": paper["paperId"],
                            "title": paper["title"],
                            "year": paper["year"]} for paper in json_res["papers"][:5] if paper is not None]
            }
        return author


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
        *(get_author_by_id(es, index, author["key"], True)
          for author in res["aggregations"]["authors_agg"]["name"]["buckets"]))

    print("get_some_authors_for_homepage result: ", top_referenced_authors)
    return top_referenced_authors
