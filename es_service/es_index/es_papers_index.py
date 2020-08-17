from es_service.es_helpers.utilites import load_json
from es_constant.constants import PAPER_DOCUMENT_INDEX, PAPERS_DATA_PATH
from es_service.es_helpers.es_connection import elasticsearch_connection
from es_service.es_helpers.es_operator import insert_doc

import os
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor


def index_paper_document(paper_document):
    paperID = paper_document.get('corpusId')
    index_document = {"corpusID": paperID,
                      "abstract": paper_document["abstract"],
                      "doi": paper_document["doi"],
                      "fieldsOfStudy": paper_document["fieldsOfStudy"],
                      "title": paper_document["title"],
                      "topics": paper_document["topics"],
                      "influentialCitationCount": paper_document["influentialCitationCount"],
                      "authors": paper_document["authors"],
                      "citations": [],
                      "references": []
                      }

    if len(paper_document["citations"]) > 0:
        for citation in paper_document["citations"]:
            index_citation = {"authors": citation["authors"],
                              "corpusID": citation["corpusID"],
                              "title": citation["title"],
                              "year": citation["year"],
                              "venue": citation["venue"],
                              "intent": citation["intent"]
                              }
            index_document["citations"].append(index_citation)

    if len(paper_document["references"]) > 0:
        for reference in paper_document["references"]:
            index_reference = {"authors": reference["authors"],
                               "corpusID": reference["corpusID"],
                               "title": reference["title"],
                               "year": reference["year"],
                               "venue": reference["venue"],
                               "intent": reference["intent"]
                               }
            index_document["references"].append(index_reference)

    es = elasticsearch_connection
    index = PAPER_DOCUMENT_INDEX

    insert_doc(es=es, index=index, id=paperID, body=index_document, verbose=True)
    print("Success paper")


def index_papers():
    executor = ThreadPoolExecutor(max_workers=20)

    for idx, file in enumerate(os.listdir(PAPERS_DATA_PATH)):
        paper_document = load_json(PAPERS_DATA_PATH + file)
        executor.submit(index_paper_document, paper_document)


if __name__ == "__main__":
    paper_document = {
          "corpusId" : 20496540,
          "abstract" : "The challenge to provide a nation-wide healthcare service continues unabated in the 21st century as politicians and managers drive through policies to modernize the UK National Health Service (NHS). Established around 60 years ago to offer free healthcare at the point of delivery to all citizens, the NHS now accounts for the largest portion of public expenditure after social security, with total spending around Â£84 billion in 2006/2007. Over the past 3 decades, the political agenda within healthcare has moved from one of professional dominance, where clinicians and their representative bodies dominated the leadership and management of healthcare organisations, to one where politicians have imposed new ideas in the form of market mechanisms and the â€œnew public managementâ€ which extend the use of private sector firms. The political justification for these reforms is to make the NHS more efficient and cost effective and to develop an ethos of patient choice.",
          "doi" : "10.4018/jcit.2008100101",
          "fieldsOfStudy" : [
            "Computer Science",
            "Engineering"
          ],
          "title" : "(NAM ADDED THIS)A Centrist Approach to Introducing ICT in Healthcare: Policies, Practices, and Pitfalls",
          "topics" : [
            {
              "topic" : "Point of delivery (networking)",
              "topicId" : "3560731",
              "url" : "https://www.semanticscholar.org/topic/3560731"
            },
            {
              "topic" : "Social security",
              "topicId" : "859610",
              "url" : "https://www.semanticscholar.org/topic/859610"
            }
          ],
          "influentialCitationCount" : 0,
          "authors" : [
            {
              "authorId" : "1870755",
              "name" : "David Jesse Finnegan",
              "url" : "https://www.semanticscholar.org/author/1870755"
            },
            {
              "authorId" : "1723170",
              "name" : "Yochai Ataria",
              "url" : "https://www.semanticscholar.org/author/1723170"
            }
          ],
          "citations" : [],
          "references" : []
        }
    elasticsearch_connection.delete(PAPER_DOCUMENT_INDEX, "k2nJ93MBeSBNH3hzXcdL")