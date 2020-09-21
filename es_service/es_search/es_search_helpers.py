from elasticsearch import NotFoundError
import httpx

HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:79.0) Gecko/20100101 Firefox/79.0",
           "Connection": "keep-alive"
           }
PROXY = httpx.Proxy(
    url="https://lum-customer-hl_26f509b3-zone-static:emgsedqdj28n@zproxy.lum-superproxy.io:22225",
    mode="TUNNEL_ONLY",
)


async def get_paper_from_id(es, index, paper_id, isInfluential=None):
    try:
        paper = es.get(index=index, id=paper_id)
        print(f"found {paper_id} on my api")
        return paper
    except NotFoundError:
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get("https://api.semanticscholar.org/v1/paper/{}".format(paper_id),
                                            headers=HEADERS)
                paper = response.json()
                if isInfluential is not None:
                    paper["isInfluential"] = isInfluential
                print(f"found {paper_id} on s2 api")
                return paper
        except Exception as e:
            print(f"paper {paper_id} failed to get {e}")
            return None


def get_paper_default_source():
    return ["paperId", "doi", "abstract", "authors", "fieldsOfStudy", "title", "topics"]


def get_paper_default_sort():
    return [{"_score": "desc"}, {"paperId.keyword": "asc"}]


def get_paper_aggregation_of_fields_of_study():
    return {
        "terms": {
            "field": "fieldsOfStudy.keyword"
        }
    }


def get_paper_aggregation_of_venues():
    return {
        "terms": {
            "field": "venue.keyword"
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


def get_author_default_source():
    return ["authorId", "aliases", "name", "influentialCitationCount", "totalPapers", "papers", "totalPapers"]


def get_author_default_sort():
    return {"_score": "desc"}


