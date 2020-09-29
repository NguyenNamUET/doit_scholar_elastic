HEADERS = {
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) SemanticScholarBot (+https://www.semanticscholar.org/crawler)",
    "Connection": "keep-alive"}
PROXY = {"https": "https://lum-customer-hl_26f509b3-zone-static:emgsedqdj28n@zproxy.lum-superproxy.io:22225"}

ES_IP = '51.210.251.250'  # 112.137.142.8 #localhost
ES_USER = 'default'  # default #user
ES_PASS = 'changeme'  # changeme #12345678
ES_PORT = '9202'  # 7778 #9202

PAPER_DOCUMENT_INDEX = 'paper'

###Description of PAPER_MAPPING: Nested authors field for aggregration
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
                    "type": "nested",
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
                        }
                    }
                },
                "authors_count": {
                    "type": "long"
                },
                "citationVelocity": {
                    "type": "long"
                },
                "citations": {
                    "properties": {
                        "doi": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                }
                            }
                        },
                        "intent": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                }
                            }
                        },
                        "isInfluential": {
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
                },
                "citations_count": {
                    "type": "long"
                },
                "corpusId": {
                    "type": "long"
                },
                "doi": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    }
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
                "paperId": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    }
                },
                "pdf_url": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    }
                },
                "references": {
                    "properties": {
                        "doi": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                }
                            }
                        },
                        "intent": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                }
                            }
                        },
                        "isInfluential": {
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
                },
                "references_count": {
                    "type": "long"
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
                "topics": {
                    "properties": {
                        "topic": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                }
                            }
                        },
                        "topicId": {
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
