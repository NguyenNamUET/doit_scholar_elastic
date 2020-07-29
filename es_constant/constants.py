PAPERS_DATA_PATH = "E:\\New folder\\papers\\"
AUTHORS_DATA_PATH = "E:\\New folder\\authors\\"
ES_IP = 'localhost'
ES_USER = 'user'
ES_PASS = '12345678'
ES_PORT = '9202'
PAPER_DOCUMENT_INDEX = 'paper'
AUTHOR_DOCUMENT_INDEX = 'author'

PAPER_MAPPING = {
    "mappings": {
        "properties": {
            "abstract": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "authors": {
                "properties": {
                    "authorId": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "name": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "url": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    }
                }
            },
            "citationVelocity": {
                "type": "long"
            },
            "corpusId": {
                "type": "long"
            },
            "fieldsOfStudy": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "influentialCitationCount": {
                "type": "long"
            },
            "is_open_access": {
                "type": "boolean"
            },
            "is_publisher_licensed": {
                "type": "boolean"
            },
            "paperId": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "title": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "url": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "venue": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "year": {
                "type": "long"
            }
        }
    }
}

AUTHOR_MAPPING = {
    "mappings": {
        "properties": {
            "authorId": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "influentialCitationCount": {
                "type": "long"
            },
            "name": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "corpusID": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "title": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "year": {
                "type": "long"
            },
            "relation_type": {
                "type": "join",
                "relations": {
                    "author": "paper"
                }
            }
        }
    }
}
