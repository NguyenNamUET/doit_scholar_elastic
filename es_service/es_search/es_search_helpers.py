def get_paper_default_source():
    return ["paperId", "doi", "abstract", "authors", "fieldsOfStudy", "title", "topics"]


def get_paper_default_sort():
    return [{"paperId.keyword": "asc"}, {"_score": "desc"}]


def count_fields_of_study_buckets():
    return {
      "cardinality": {
        "field": "fieldsOfStudy.keyword"
      }
    }


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


