#Description of data:
###a) Paper data:
**Fields:**\
_corpusID_: id của paper\
_abstract"_: tóm tắt của paper\
_fieldsOfStudy_: lĩnh vực của paper\
_title_: tiêu đề của paper\
_topics_: chủ đề đề cập trong paper\
_influentialCitationCount_: số lượng trích dẫn\
_authors_: các tác giả của paper\
_citations_: các trích dẫn của paper\
_references_: các đề cập của paper\

**Elasticsearch mapping**: Nested 3 fields **citations**, **references**, **authors**
Query result:\
Author aggregation cho ra kết quả:
```json
{
  "author_count": {
    "doc_count": 69,
    "name": {
      "doc_count_error_upper_bound": 0,
      "sum_other_doc_count": 59,
      "buckets": [
        {
          "key": "100659372",
          "doc_count": 1,
          "name": {
            "doc_count_error_upper_bound": 0,
            "sum_other_doc_count": 0,
            "buckets": [
              {
                "key": "Harold Joseph Noble",
                "doc_count": 1
              }
            ]
          }
        }
      ]
    }
  }
}
```
Field aggregation cho ra kết quả:
```json
{
    "fields_of_study": {
      "doc_count_error_upper_bound": 0,
      "sum_other_doc_count": 0,
      "buckets": [
        {
          "key": "Engineering",
          "doc_count": 28
        },
        {
          "key": "Computer Science",
          "doc_count": 2
        },
        {
          "key": "Medicine",
          "doc_count": 2
        }
      ]
    }
}
```