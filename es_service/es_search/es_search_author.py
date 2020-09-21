from es_service.es_helpers.es_connection import elasticsearch_connection
from es_service.es_constant.constants import HEADERS, PROXY

import requests


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


def get_author_by_id(es, index, author_id):
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
          "from": 0,
          "size": 5,
          "_source": ["paperId", "title", "fieldsOfStudy", "authors", "venue", "year"],
          "aggs": {
            "influentialCitationCount": {
                "sum": {
                    "field": "influentialCitationCount"
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
        author = {
                    "authorId":	author_id,
                    "influentialCitationCount":	res["aggregations"]["influentialCitationCount"]["value"],
                    "totalPapers": res["aggregations"]["totalPapers"]["value"],
                    "name":	[p["name"] for p in res["hits"]["hits"][0]["_source"]["authors"] if p["authorId"] == author_id][0],
                    "papers": [p["_source"] for p in res["hits"]["hits"]]
                 }
        return author
    else:
        print(f"NOT FOUND AUTHOR {author_id} ON MY API")
        response = requests.get("https://api.semanticscholar.org/v1/author/{}".format(author_id),
                                headers=HEADERS, proxies=PROXY)
        json_res = response.json()
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



def get_author_by_name(es, index, author_name):
    pass


if __name__ == "__main__":
    print(count_authors(elasticsearch_connection, "paper"))
