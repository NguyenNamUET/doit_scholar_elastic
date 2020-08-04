{"query": {"bool": {"should": [{"match": {"fieldsOfStudy.keyword": {"query": "Engineering"}}}]}}, "from": 0, "size": 10, "_source": ["corpusID", "abstract", "authors", "fieldsOfStudy", "title", "topics"], "sort": [{"_score": "desc"}], "aggs": {"author_count": {"nested": {"path": "authors"}, "aggs": {"name": {"terms": {"field": "authors.authorId.keyword", "size": 10}, "aggs": {"name": {"terms": {"field": "authors.name.keyword"}}}}}}, "fields_of_study": {"terms": {"field": "fieldsOfStudy.keyword"}}}}
