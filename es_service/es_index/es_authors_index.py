from es_service.es_helpers.utilites import load_json
from es_constant.constants import AUTHOR_DOCUMENT_INDEX, AUTHORS_DATA_PATH
from es_service.es_helpers.es_connection import elasticsearch_connection
from es_service.es_helpers.es_operator import insert_doc

import os
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor


def index_author_document(author_document):
    index_document = {"authorId": author_document["authorId"],
                      "aliases": author_document["aliases"],
                      "name": author_document["name"],
                      "influentialCitationCount": author_document["influentialCitationCount"],
                      "totalPapers": len(author_document["papers"]),
                      "papers": []
                      }

    if len(author_document["papers"]) > 0:
        for paper in author_document["papers"]:
            index_paper = {"corpusID": paper["corpusID"],
                           "title": paper["title"],
                           "year": paper["year"]
                           }
            index_document["papers"].append(index_paper)

    es = elasticsearch_connection
    index = AUTHOR_DOCUMENT_INDEX
    authorID = author_document.get('authorId')
    insert_doc(es=es, index=index, id=authorID, body=index_document, verbose=True)
    print("Success author")


def index_authors():
    executor = ThreadPoolExecutor(max_workers=20)

    for idx, file in enumerate(os.listdir(AUTHORS_DATA_PATH)):
        author_document = load_json(AUTHORS_DATA_PATH + file)
        executor.submit(index_author_document, author_document)


if __name__ == "__main__":
    # authorID = 1702520 & 1828961
    # Overlap: paperID = 2708220
    # author_document = load_json("/home/nguyennam/Downloads/Semantic/Semantic Self Extracted Data 2/authors/author_1699095.json")
    # print(author_document)
    index_authors()
