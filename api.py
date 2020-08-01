from typing import Optional, List
from fastapi import FastAPI, Query
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware

from es_service.es_helpers.es_connection import elasticsearch_connection
from es_constant.constants import PAPER_DOCUMENT_INDEX
from es_constant.constants import AUTHOR_DOCUMENT_INDEX

from es_service.es_search.es_search import get_paper_by_id, get_all_papers, get_all_fields_of_study, \
    search_paper_title, search_paper_abstract, get_all_topics, get_paper_by_topic
from es_service.es_search.es_search_author import get_author_by_id, get_author_by_name

app = FastAPI()

origins = [
    "http://localhost:3000",
    "https://localhost:3000"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Run command: uvicorn api:app --reload

class paperItem(BaseModel):
    searchContent: str
    start: Optional[int] = 0
    size: Optional[int] = 10
    return_top_author: Optional[bool] = False
    top_author_size: Optional[int] = 10


class authorItem(BaseModel):
    author_name: str


@app.get("/s2api/papers/{paperID}")
def getPaperByID(paperID: int):
    result = get_paper_by_id(elasticsearch_connection, PAPER_DOCUMENT_INDEX, paperID)
    return result


@app.post("/s2api/papers/getAllpapers")
def getAllPapers(query: paperItem):
    result = get_all_papers(elasticsearch_connection, PAPER_DOCUMENT_INDEX, query.start, query.size)
    return result


@app.post("/s2api/papers/getAllFieldOfStudy")
def getAllFieldOfStudy():
    result = get_all_fields_of_study(elasticsearch_connection, PAPER_DOCUMENT_INDEX)
    return result


@app.post("/s2api/papers/searchPaperTitle")
def searchPaperTitle(query: paperItem):
    result = search_paper_title(search_content=query.searchContent,
                                es=elasticsearch_connection,
                                index=PAPER_DOCUMENT_INDEX,
                                start=query.start,
                                size=query.size,
                                return_top_author=query.return_top_author,
                                top_author_size=query.top_author_size)
    return result


@app.post("/s2api/papers/searchPaperAbstract")
def searchPaperAbstract(query: paperItem):
    result = search_paper_abstract(search_content=query.searchContent,
                                   es=elasticsearch_connection,
                                   index=PAPER_DOCUMENT_INDEX,
                                   start=query.start,
                                   size=query.size,
                                   return_top_author=query.return_top_author,
                                   top_author_size=query.top_author_size)
    return result


@app.post("/s2api/papers/getAllTopics")
def getAllTopics():
    result = get_all_topics(es=elasticsearch_connection, index=PAPER_DOCUMENT_INDEX)
    return result


@app.post("/s2api/papers/getPaperByTopic")
def getPaperByTopic(topics: List[str] = Query(None)):
    result = get_paper_by_topic(es=elasticsearch_connection,
                                index=PAPER_DOCUMENT_INDEX,
                                topic=topics)
    return result


# All authors api

@app.get("/s2api/authors/{author_id}")
def getAuthorById(author_id: str):
    result = get_author_by_id(es=elasticsearch_connection,
                              index=AUTHOR_DOCUMENT_INDEX,
                              id=author_id)
    return result


@app.post("/s2api/authors/getAuthorByName")
def getAuthorByName(query: authorItem):
    result = get_author_by_name(es=elasticsearch_connection,
                                index=AUTHOR_DOCUMENT_INDEX,
                                name=query.author_name)
    return result
