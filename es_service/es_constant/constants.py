ES_IP = 'localhost' #112.137.142.8 #localhost
ES_USER = 'user' #default #user
ES_PASS = '12345678' #changeme #12345678
ES_PORT = '9202' #7778 #9202

#####
PAPER_DOCUMENT_INDEX = 'paper'

###Description of PAPER_MAPPING: Nested authors field for aggregration
PAPER_MAPPING = {
    "mappings" : {
      "properties" : {
        "abstract" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "authors" : {
          "type": "nested",
          "properties" : {
            "authorId" : {
              "type" : "text",
              "fields" : {
                "keyword" : {
                  "type" : "keyword",
                  "ignore_above" : 256
                }
              }
            },
            "name" : {
              "type" : "text",
              "fields" : {
                "keyword" : {
                  "type" : "keyword",
                  "ignore_above" : 256
                }
              }
            }
          }
        },
        "citationVelocity" : {
          "type" : "long"
        },
        "citations" : {
          "properties" : {
            "intent" : {
              "type" : "text",
              "fields" : {
                "keyword" : {
                  "type" : "keyword",
                  "ignore_above" : 256
                }
              }
            },
            "isInfluential" : {
              "type" : "boolean"
            },
            "paperId" : {
              "type" : "text",
              "fields" : {
                "keyword" : {
                  "type" : "keyword",
                  "ignore_above" : 256
                }
              }
            }
          }
        },
        "citations_count" : {
          "type" : "long"
        },
        "corpusId" : {
          "type" : "long"
        },
        "doi" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "fieldsOfStudy" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "influentialCitationCount" : {
          "type" : "long"
        },
        "paperId" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "pdf_url" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "references" : {
          "properties" : {
            "intent" : {
              "type" : "text",
              "fields" : {
                "keyword" : {
                  "type" : "keyword",
                  "ignore_above" : 256
                }
              }
            },
            "isInfluential" : {
              "type" : "boolean"
            },
            "paperId" : {
              "type" : "text",
              "fields" : {
                "keyword" : {
                  "type" : "keyword",
                  "ignore_above" : 256
                }
              }
            }
          }
        },
        "references_count" : {
          "type" : "long"
        },
        "title" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "venue" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "year" : {
          "type" : "long"
        }
      }
    }
  }

