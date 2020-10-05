from es_service.es_helpers.es_connection import elasticsearch_connection
from es_service.es_constant.constants import HEADERS, PROXY
from es_service.es_search.es_search_helpers import get_paper_aggregation_of_authors

import requests
import asyncio

def count_authors(es, index):
    query = {
      "size": 0,
      "aggs": {
        "author_aggs": {
          "nested": {
            "path": "authors"
          },
          "aggs": {
            "author_count":{
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
                "authors.authorId.keyword": author_id #150080110
              }
          }
        }
      },
      "from": start,
      "size": size,
      "_source": ["paperId", "title", "fieldsOfStudy", "authors", "venue", "year"]
    }

    res = es.search(index=index, body=query)
    print("get_some_papers result: ", res)
    return [p["_source"] for p in res["hits"]["hits"]]


async def get_author_by_id(es, index, author_id, shorted=False):
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
          "_source": ["paperId", "title", "fieldsOfStudy", "authors", "venue", "year", "references_count"],
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
          }
    }
    res = es.search(index=index, body=query)
    if res["hits"]["total"]["value"] > 0:
        print(f"FOUND AUTHOR {author_id} ON ELASTIC")
        if shorted:
            author = {
                    "authorId":	author_id,
                    "influentialCitationCount":	res["aggregations"]["influentialCitationCount"]["value"],
                    "totalPapers": res["aggregations"]["totalPapers"]["value"],
                    "citationsCount": res["aggregations"]["citationsCount"]["value"],
                    "name":	[p["name"] for p in res["hits"]["hits"][0]["_source"]["authors"] if p["authorId"] == author_id][0]
                    }
        else:
            author = {
                    "authorId":	author_id,
                    "influentialCitationCount":	res["aggregations"]["influentialCitationCount"]["value"],
                    "totalPapers": res["aggregations"]["totalPapers"]["value"],
                    "citationsCount": res["aggregations"]["citationsCount"]["value"],
                    "name":	[p["name"] for p in res["hits"]["hits"][0]["_source"]["authors"] if p["authorId"] == author_id][0],
                    "papers": [p["_source"] for p in res["hits"]["hits"]]
                 }
        return author
    else:
        print(f"NOT FOUND AUTHOR {author_id} ON MY API")
        response = requests.get("https://api.semanticscholar.org/v1/author/{}".format(author_id),
                                headers=HEADERS, proxies=PROXY)
        json_res = response.json()
        if shorted:
            author = {
                "authorId": author_id,
                "influentialCitationCount": json_res["influentialCitationCount"],
                "citationsCount": 0,
                "totalPapers": len(json_res["papers"]),
                "name": json_res["name"]
            }
        else:
            author = {
                "authorId": author_id,
                "influentialCitationCount": json_res["influentialCitationCount"],
                "totalPapers": len(json_res["papers"]),
                "name": json_res["name"],
                "papers": [{"paperId":paper["paperId"],
                            "title": paper["title"],
                            "year": paper["year"]} for paper in json_res["papers"][:5]]
            }
        return author


##################################### HOMEPAGE FUNCTION ###############################################
async def get_some_authors_for_homepage(es, index, size=3):
    p_query = {
              "query":{
                "match_all" : {}
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

