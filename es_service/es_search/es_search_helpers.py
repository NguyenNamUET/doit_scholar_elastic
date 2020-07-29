def get_paper_default_source():
    return ["corpusID", "abstract", "authors", "fieldsOfStudy", "title", "topics"]


def get_paper_default_sort():
    return {"_score": "desc"}


def get_paper_aggregation_of_fields_of_study():
    return {
        "fields_of_study": {
            "terms": {
                "field": "fieldsOfStudy.keyword"
            }
        }
    }


def get_author_default_source():
    return ["authorId", "aliases", "name", "influentialCitationCount", "totalPapers", "papers", "totalPapers"]


def get_author_default_sort():
    return {"_score": "desc"}
