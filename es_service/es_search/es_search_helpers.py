def get_paper_default_source():
    return ["corpusID", "abstract", "authors", "fieldsOfStudy"]


def get_paper_default_sort():
    return {"_score":"asc"}


def get_paper_aggregation_of_fields_of_study():
    return {
            "fields_of_study":{
              "terms":{
                "field": "fieldsOfStudy.keyword"
              }
            }
          }
