from typing import Optional
from fastapi import FastAPI
from pydantic import BaseModel

from es_service.es_helpers.es_connection import elasticsearch_connection
from es_constant.constants import PAPER_DOCUMENT_INDEX

from es_service.es_search.es_search import get_paper_by_id, get_all_papers, get_all_fields_of_study, \
                                            search_paper_title, search_paper_abstract

app = FastAPI()

#Run command: uvicorn api:app --reload
@app.get("/s2api/papers/{paperID}")
def getPaperByID(paperID:int):
    result = get_paper_by_id(elasticsearch_connection, PAPER_DOCUMENT_INDEX, paperID)
    return result


@app.post("/s2api/papers/getAllpapers")
def getAllpapers(start: Optional[int] = 0, size: Optional[int] = 10):
    result = get_all_papers(elasticsearch_connection, PAPER_DOCUMENT_INDEX, start, size)
    return result


@app.post("/s2api/papers/getAllFieldOfStudy")
def getAllFieldOfStudy():
    result = get_all_fields_of_study(elasticsearch_connection, PAPER_DOCUMENT_INDEX)
    return result


@app.post("/s2api/papers/searchPaperTitle")
def searchPaperTitle(searchContent: str, start: Optional[int] = 0, size: Optional[int] = 10,
                     return_top_author: Optional[bool] = False, top_author_size: Optional[int] = 10):
    result = search_paper_title(search_content=searchContent,
                                es=elasticsearch_connection,
                                index=PAPER_DOCUMENT_INDEX,
                                start=start,
                                size=size,
                                return_top_author=return_top_author,
                                top_author_size=top_author_size)
    return result


@app.post("/s2api/papers/searchPaperAbstract")
def searchPaperAbstract(searchContent: str, start: Optional[int] = 0, size: Optional[int] = 10,
                        return_top_author: Optional[bool] = False, top_author_size: Optional[int] = 10):
    result = search_paper_abstract(search_content=searchContent,
                                   es=elasticsearch_connection,
                                   index=PAPER_DOCUMENT_INDEX,
                                   start=start,
                                   size=size,
                                   return_top_author=return_top_author,
                                   top_author_size=top_author_size)
    return result
