from es_service.es_helpers.utilites import load_json
from es_constant.constants import PAPER_DOCUMENT_INDEX, PAPERS_DATA_PATH
from es_service.es_helpers.es_connection import elasticsearch_connection
from es_service.es_helpers.es_operator import insert_doc

import os
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor


def index_paper_document(paper_document):
    index_document = {"abstract": paper_document["abstract"],
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

    index_document["references"] = []
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
    paperID = paper_document.get('corpusId')
    insert_doc(es=es, index=index, id=paperID, body=index_document, verbose=True)
    print("Success paper")


def index_papers():
    executor = ThreadPoolExecutor(max_workers=20)

    for idx, file in enumerate(os.listdir(PAPERS_DATA_PATH)):
        paper_document = load_json(PAPERS_DATA_PATH + file)
        executor.submit(index_paper_document, paper_document)


if __name__ == "__main__":
    # paper_document = load_json("/home/nguyennam/Downloads/Semantic/Semantic Self Extracted Data 2/papers/paper_2420401.json")
    # print(paper_document)
    index_papers()
