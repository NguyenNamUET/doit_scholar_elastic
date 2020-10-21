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
    cit_counter = Counter(cit['year'] for cit in citations if cit['year'] is not None)
    cit_aggs = {year:count for year, count in sorted(cit_counter.most_common(size))}

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