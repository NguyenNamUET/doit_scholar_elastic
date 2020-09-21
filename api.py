from typing import Optional, List
from fastapi import FastAPI, Query, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware

from es_service.es_helpers.es_connection import elasticsearch_connection
from es_service.es_constant.constants import PAPER_DOCUMENT_INDEX

from es_service.es_search.es_search_paper import get_paper_by_id, count_papers, count_fields_of_study, \
    count_topics, get_some_citations, get_some_references
from es_service.es_search.es_search_paper import search_by_title, search_by_abstract, search_by_fields_of_study, \
    search_by_topics, search_on_typing
from es_service.es_search.es_search_author import count_authors, get_author_by_id, get_some_papers, get_author_by_name

import asyncio

app = FastAPI()

origins = [
    "http://localhost",
    "http://localhost:3000",
    "https://localhost:3000",
    "http://127.0.0.1:8000",
    "http://localhost:8000",
    # "http://112.137.142.8:7778",
    # "http://112.137.142.8:3400"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content=jsonable_encoder({"detail": exc.errors(), "body": exc.body}),
    )


# Run command: uvicorn api:app --reload

class paperItem(BaseModel):
    search_content: Optional[str] = "neural"
    authors: Optional[List[str]] = Query(None)
    topics: Optional[List[str]] = Query(None)
    venues: Optional[List[str]] = Query(None)
    fields_of_study: Optional[List[str]] = Query(None)
    # Condion for fields match (False=must, True=should)
    topic_is_should: Optional[bool] = True
    fos_is_should: Optional[bool] = False
    author_is_should: Optional[bool] = False
    venues_is_should: Optional[bool] = False
    # Fields of study aggs
    return_fos_aggs: Optional[bool] = False
    # Venues aggs
    return_venue_aggs: Optional[bool] = False
    # Pagination
    deep_pagination: Optional[bool] = False
    last_paper_id: Optional[int] = 0
    # Top author aggs
    return_top_author: Optional[bool] = False
    top_author_size: Optional[int] = 10
    # Optional parameters
    start: Optional[int] = 0
    size: Optional[int] = 10
    source: Optional[List[str]] = Query(None)
    sort_by: Optional[str] = Query(None)


class authorItem(BaseModel):
    author_name: Optional[str] = ""
    # Optional parameters
    start: Optional[int] = 0
    size: Optional[int] = 10
    source: Optional[List[str]] = Query(None)
    sort_by: Optional[str] = Query(None)


################################# All papers api ###########################
@app.get("/s2api/papers/{paperID}")
async def getpaperByID(paperID: str):
    result = await get_paper_by_id(es=elasticsearch_connection,
                                   index=PAPER_DOCUMENT_INDEX,
                                   paper_id=paperID)
    return result


@app.post("/s2api/papers/countPapers")
def countPapers():
    result = count_papers(es=elasticsearch_connection,
                          index=PAPER_DOCUMENT_INDEX)
    return result


@app.post("/s2api/papers/countFOS")
def countFOS():
    result = count_fields_of_study(es=elasticsearch_connection,
                                   index=PAPER_DOCUMENT_INDEX)
    return result


@app.post("/s2api/papers/countTopics")
def countTopics():
    result = count_topics(es=elasticsearch_connection,
                          index=PAPER_DOCUMENT_INDEX)
    return result


@app.post("/s2api/papers/searchPaperTitle")
def searchPaperTitle(query: paperItem):
    result = search_by_title(es=elasticsearch_connection, index=PAPER_DOCUMENT_INDEX,
                             search_content=query.search_content,
                             venues=query.venues, venues_isShould=query.venues_is_should,
                             authors=query.authors, author_isShould=query.author_is_should,
                             fields_of_study=query.fields_of_study, fos_isShould=query.fos_is_should,
                             start=query.start, size=query.size, source=query.source, sort_by=query.sort_by,
                             return_fos_aggs=query.return_fos_aggs,
                             return_venue_aggs=query.return_venue_aggs,
                             deep_pagination=query.deep_pagination, last_paper_id=query.last_paper_id,
                             return_top_author=query.return_top_author, top_author_size=query.top_author_size)

    return result


@app.post("/s2api/papers/searchPaperAbstract")
def searchPaperAbstract(query: paperItem):
    result = search_by_abstract(es=elasticsearch_connection, index=PAPER_DOCUMENT_INDEX,
                                search_content=query.search_content,
                                start=query.start, size=query.size, source=query.source, sort_by=query.sort_by,
                                return_fos_aggs=query.return_fos_aggs,
                                deep_pagination=query.deep_pagination, last_paper_id=query.last_paper_id,
                                return_top_author=query.return_top_author, top_author_size=query.top_author_size)

    return result


@app.post("/s2api/papers/searchPaperFOS")
def searchPaperFOS(query: paperItem):
    result = search_by_fields_of_study(es=elasticsearch_connection, index=PAPER_DOCUMENT_INDEX,
                                       fields_of_study=query.fields_of_study, fos_isShould=query.fos_is_should,
                                       start=query.start, size=query.size, source=query.source, sort_by=query.sort_by,
                                       return_fos_aggs=query.return_fos_aggs,
                                       deep_pagination=query.deep_pagination, last_paper_id=query.last_paper_id,
                                       return_top_author=query.return_top_author, top_author_size=query.top_author_size)

    return result


@app.post("/s2api/papers/searchPaperByTopics")
def searchPaperByTopics(query: paperItem):
    result = search_by_topics(es=elasticsearch_connection, index=PAPER_DOCUMENT_INDEX,
                              topics=query.topics, topic_isShould=query.topic_is_should,
                              start=query.start, size=query.size, source=query.source, sort_by=query.sort_by,
                              return_fos_aggs=query.return_fos_aggs,
                              deep_pagination=query.deep_pagination, last_paper_id=query.last_paper_id,
                              return_top_author=query.return_top_author, top_author_size=query.top_author_size)

    return result


@app.get("/s2api/papers/{paperID}/citations")
async def getSomeCitations(paperID: str, start: Optional[int] = 0, size: Optional[int] = 5):
    result = await get_some_citations(es=elasticsearch_connection, index=PAPER_DOCUMENT_INDEX,
                                      paper_id=paperID,
                                      start=start, size=size)

    return result


@app.get("/s2api/papers/{paperID}/references")
async def getSomeReferences(paperID: str, start: Optional[int] = 0, size: Optional[int] = 5):
    result = await get_some_references(es=elasticsearch_connection, index=PAPER_DOCUMENT_INDEX,
                                       paper_id=paperID,
                                       start=start, size=size)

    return result


@app.post("/s2api/papers/searchOnTyping")
def searchOnTyping(query: paperItem):
    result = search_on_typing(es=elasticsearch_connection, index=PAPER_DOCUMENT_INDEX,
                              search_content=query.search_content, size=query.size)

    return result


################################# All authors api ###########################
@app.get("/s2api/authors/countAuthors")
def countAuthors():
    result = count_authors(es=elasticsearch_connection,
                           index=PAPER_DOCUMENT_INDEX)
    return result


@app.get("/s2api/authors/{author_id}")
def getAuthorById(author_id: str):
    result = get_author_by_id(es=elasticsearch_connection,
                              index=PAPER_DOCUMENT_INDEX,
                              author_id=author_id)
    return result


@app.post("/s2api/authors/getAuthorByName")
def getAuthorByName(query: authorItem):
    result = get_author_by_name(es=elasticsearch_connection,
                                index=PAPER_DOCUMENT_INDEX,
                                author_name=query.author_name)
    return result


@app.get("/s2api/papers/{author_id}/papers")
def getSomePapers(author_id: str, start: Optional[int] = 0, size: Optional[int] = 5):
    result = get_some_papers(es=elasticsearch_connection, index=PAPER_DOCUMENT_INDEX,
                             author_id=author_id,
                             start=start, size=size)

    return result
