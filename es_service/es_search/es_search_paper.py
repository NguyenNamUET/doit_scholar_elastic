from elasticsearch import NotFoundError

from es_service.es_helpers.es_connection import elasticsearch_connection

from es_service.es_search.es_search_helpers import get_paper_default_source, get_paper_aggregation_of_authors, \
    get_paper_aggregation_of_fields_of_study, get_paper_default_sort, count_fields_of_study_buckets, \
    get_paper_aggregation_of_venues


##Straight forward functions (no building query by hand)
def get_paper_by_id(es, index, paper_id):
    try:
        paper = es.get(index=index, id=paper_id)
        res = {"doi": paper['_source']["doi"],
               "corpusId": paper['_source']["corpusId"],
               "title": paper['_source']["title"],
               "venue": paper['_source']["venue"],
               "year": paper['_source']["year"],
               "abstract": paper['_source']["abstract"],
               "authors": paper['_source']["authors"],
               "fieldsOfStudy": paper['_source']["fieldsOfStudy"],
               "topics": paper['_source']["topics"],
               "citationVelocity": paper['_source']["citationVelocity"],
               "citations": paper['_source']["citations"][:5],
               "references": paper['_source']["references"][:5],
               "citations_length": len(paper['_source']["citations"]),
               "references_length": len(paper['_source']["references"])
               }
        return res
    except NotFoundError:
        print('not found')
        return {}


# These builder function only return part of query
# We will assemble them later
def common_query__builder(start=0, size=10, source=None, sort_by=None,
                          return_top_author=False, top_author_size=10,
                          return_fos_aggs=False,
                          return_venue_aggs=False,
                          deep_pagination=False, last_paper_id=None):
    if source is None:
        source = get_paper_default_source()

    if sort_by is None:
        sort_by = get_paper_default_sort()

    query = {"from": start,
             "size": size,
             "aggs": {},
             "_source": source,
             "sort": sort_by}

    if deep_pagination:
        query["search_after"] = [last_paper_id, 0]
        query["from"] = 0

    if return_top_author:
        query["aggs"]["author_count"] = get_paper_aggregation_of_authors(size=top_author_size)

    if return_fos_aggs:
        query["aggs"]["fos_count"] = get_paper_aggregation_of_fields_of_study()

    if return_venue_aggs:
        query["aggs"]["venue_count"] = get_paper_aggregation_of_venues()

    print("common_query__builder: ", query)
    return query


def search_paper_title__builder(search_content):
    query = {
        "match": {
            "title": {
                "query": search_content,
                "fuzziness": 2
            }
        }
    }
    print("search_paper_title__builder: ", query)
    return query


def search_paper_abstract__builder(search_content):
    query = {
        "match": {
            "abstract": {
                "query": search_content,
                "fuzziness": 1
            }
        }
    }
    print("search_paper_abstract__builder: ", query)
    return query


def search_paper_by_topics__builder(topics, topic_isShould=True):
    query = {
        "bool": {
            "should": []
        }
    }
    for topic in topics:
        query["bool"]["should"].append({
            "match": {
                "topics.topic.keyword": {
                    "query": topic
                }
            }
        })
    print("search_paper_by_topics__builder: ", query)
    return query


def search_paper_by_fos__builder(fields_of_study, fos_isShould=True):
    if fos_isShould:
        query = {
            "bool": {
                "should": []
            }
        }
        for fos in fields_of_study:
            query["bool"]["should"].append({
                "match": {
                    "fieldsOfStudy.keyword": {
                        "query": fos
                    }
                }
            })
    else:
        query = {
            "bool": {
                "must": []
            }
        }
        for fos in fields_of_study:
            query["bool"]["must"].append({
                "match": {
                    "fieldsOfStudy.keyword": {
                        "query": fos
                    }
                }
            })
    print("search_paper_by_fos__builder: ", query)
    return query


def search_paper_by_venues__builder(venues, venues_isShould=True):
    if venues_isShould:
        query = {
            "bool": {
                "should": []
            }
        }
        for venue in venues:
            query["bool"]["should"].append({
                "match": {
                    "venue.keyword": {
                        "query": venue
                    }
                }
            })
    else:
        query = {
            "bool": {
                "must": []
            }
        }
        for venue in venues:
            query["bool"]["must"].append({
                "match": {
                    "venue.keyword": {
                        "query": venue
                    }
                }
            })
    print("search_paper_by_venues__builder: ", query)
    return query


def search_by_author__builder(authors, author_isShould):
    if author_isShould:
        query = {
            "nested": {
                "path": "authors",
                "query": {
                    "bool": {
                        "should": []
                    }
                }
            }
        }
        for author in authors:
            query["nested"]["query"]["bool"]["should"].append(
                {
                    "match": {
                        "authors.name.keyword": {
                            "query": author
                        }
                    }
                }
            )
    else:
        query = {"query": []}
        for author in authors:
            query["query"].append({
                "nested": {
                    "path": "authors",
                    "query": {
                        "match": {
                            "authors.name.keyword": {
                                "query": author
                            }
                        }
                    }
                }
            })

    print("search_by_author__builder: ", query)
    return query


####I assemble these builder here to create function
def search_by_title(es, index, search_content,
                    venues=None, venues_isShould=False,
                    authors=None, author_isShould=False,
                    fields_of_study=None, fos_isShould=True,
                    start=0, size=10, source=None, sort_by=None,
                    return_fos_aggs=False,
                    return_venue_aggs=False,
                    deep_pagination=False, last_paper_id=None,
                    return_top_author=False, top_author_size=10):
    common_query = common_query__builder(start=start, size=size, source=source, sort_by=sort_by,
                                         return_top_author=return_top_author, top_author_size=top_author_size,
                                         return_fos_aggs=return_fos_aggs,
                                         return_venue_aggs=return_venue_aggs,
                                         deep_pagination=deep_pagination, last_paper_id=last_paper_id)
    title_query = search_paper_title__builder(search_content=search_content)
    query = {"query":
                 {"bool":
                      {"must": []}
                  }
             }
    query["query"]["bool"]["must"].append(title_query)

    if venues is not None:
        venues_query = search_paper_by_venues__builder(venues=venues, venues_isShould=venues_isShould)

        query["query"]["bool"]["must"].append(venues_query)

    if fields_of_study is not None:
        fos_query = search_paper_by_fos__builder(fields_of_study=fields_of_study,
                                                 fos_isShould=fos_isShould)

        query["query"]["bool"]["must"].append(fos_query)

    if authors is not None:
        authors_query = search_by_author__builder(authors=authors,
                                                  author_isShould=author_isShould)

        if author_isShould:
            query["query"]["bool"]["must"].append(authors_query)
        else:
            for author_query in authors_query["query"]:
                query["query"]["bool"]["must"].append(author_query)

    query.update(common_query)
    print("search_by_title query: ", query)

    result = es.search(index=index, body=query)
    print("search_by_title result: ", result)
    if result["hits"]["total"]["value"] == 0:
        return {}

    return result



def search_by_abstract(es, index, search_content,
                       start=0, size=10, source=None, sort_by=None,
                       return_fos_aggs=False,
                       deep_pagination=False, last_paper_id=None,
                       return_top_author=False, top_author_size=10):
    common_query = common_query__builder(start=start, size=size, source=source, sort_by=sort_by,
                                         return_top_author=return_top_author, top_author_size=top_author_size,
                                         return_fos_aggs=return_fos_aggs,
                                         deep_pagination=deep_pagination, last_paper_id=last_paper_id)
    abstract_query = search_paper_abstract__builder(search_content=search_content)
    query = {"query": abstract_query}

    query.update(common_query)
    print("search_by_abstract query: ", query)

    result = es.search(index=index, body=query)
    print("search_by_abstract result: ", result)
    if result["hits"]["total"]["value"] == 0:
        return {}

    return result


def search_by_fields_of_study(es, index,
                              fields_of_study=None, fos_isShould=True,
                              start=0, size=10, source=None, sort_by=None,
                              return_fos_aggs=False,
                              deep_pagination=False, last_paper_id=None,
                              return_top_author=False, top_author_size=10):
    common_query = common_query__builder(start=start, size=size, source=source, sort_by=sort_by,
                                         return_top_author=return_top_author, top_author_size=top_author_size,
                                         return_fos_aggs=return_fos_aggs,
                                         deep_pagination=deep_pagination, last_paper_id=last_paper_id)
    fos_query = search_paper_by_fos__builder(fields_of_study=fields_of_study,
                                             fos_isShould=fos_isShould)
    query = {"query": fos_query}
    query.update(common_query)
    print("search_by_fields_of_study query: ", query)

    result = es.search(index=index, body=query)
    print("search_by_fields_of_study result: ", result)
    if result["hits"]["total"]["value"] == 0:
        return {}

    return result.hits.hits


def search_by_topics(es, index,
                     topics=None, topic_isShould=True,
                     start=0, size=10, source=None, sort_by=None,
                     return_fos_aggs=False,
                     deep_pagination=False, last_paper_id=None,
                     return_top_author=False, top_author_size=10):
    common_query = common_query__builder(start=start, size=size, source=source, sort_by=sort_by,
                                         return_top_author=return_top_author, top_author_size=top_author_size,
                                         return_fos_aggs=return_fos_aggs,
                                         deep_pagination=deep_pagination, last_paper_id=last_paper_id)
    topic_query = search_paper_by_topics__builder(topics=topics,
                                                  topic_isShould=topic_isShould)
    query = {"query": topic_query}
    query.update(common_query)
    print("search_by_topics query: ", query)

    result = es.search(index=index, body=query)
    print("search_by_topics result: ", result)
    if result["hits"]["total"]["value"] == 0:
        return {}

    return result


def search_on_typing(es, index, search_content, size=10):
    common_query = common_query__builder(source=["title", "citations_count"], sort_by=[{"citations_count": "desc"}])
    query = {
        "query": {
            "match": {
                "title": {
                    "query": search_content
                }
            }
        }
    }
    query.update(common_query)
    print("search_on_typing query: ", query)

    result = es.search(index=index, body=query)
    if result["hits"]["total"]["value"] == 0:
        return {}

    print("search_on_typing result: ", result["hits"]["hits"])
    return result["hits"]["hits"]


# These functions are merely for counting (no complex search)
def get_all_fields_of_study(es, index, size=10):
    query = {
        "size": 0,
        "aggs": {
            "fields_of_study": get_paper_aggregation_of_fields_of_study(),
            "fos_unique_count": count_fields_of_study_buckets()
        }
    }

    print("Get all fields of study query :", query)
    result = es.search(index=index, body=query)
    print("Get all fields of study result :", result)
    return result["aggregations"]


def get_all_papers(es, index,
                   start=0, size=10, source=None, sort_by=None,
                   return_fos_aggs=False,
                   deep_pagination=False, last_paper_id=None,
                   return_top_author=False, top_author_size=10):
    common_query = common_query__builder(start=start, size=size, source=source, sort_by=sort_by,
                                         return_top_author=return_top_author,
                                         top_author_size=top_author_size,
                                         return_fos_aggs=return_fos_aggs,
                                         deep_pagination=deep_pagination, last_paper_id=last_paper_id)
    query = {
        "query": {
            "match_all": {}
        },

    }
    query.update(common_query)
    print("Get all papers query :", query)
    result = es.search(index=index, body=query)
    print("Get all papers result :", result)
    return result["hits"]


def get_all_topics(es, index):
    query = {
        "size": 0,
        "aggs": {
            "topics": {
                "terms": {
                    "field": "topics.topic.keyword"
                }
            }
        }
    }
    print('Get all topics query: ', query)
    result = es.search(index=index, body=query)
    print('Get all topics result: ', result)
    return result['aggregations']['topics']


def get_some_citations(es, index,
                       paper_id,
                       start=0, size=5):
    try:
        res = es.get(index=index, id=paper_id)
        return res["_source"]["citations"][start:start + size]
    except NotFoundError:
        print('paper {} not found'.format(paper_id))
        return {}


def get_some_references(es, index,
                        paper_id,
                        start=0, size=5):
    try:
        res = es.get(index=index, id=paper_id)
        return res["_source"]["references"][start:start + size]
    except NotFoundError:
        print('paper {} not found'.format(paper_id))
        return {}


def get_some_citations_2(es, index,
                         paper_id,
                         start=0, size=5):
    try:
        # paper = es.get(index=index, id=paper_id)
        # paper_ids = [citation["paperId"] for citation in paper["_source"]["citations"][start:start + size]]
        # print(paper_ids)
        paper_ids = ['0022759d8fdafd1ceb5786a86ada294650d9b791', '009636137148198eee39937e218cd0ca9609caaa', '02063cf5d5badba70f998f01038155aa16d6a659', '0224c5594764a6b850f5d1ad83516bed8f90d3d9', '02cfbfcac571d5e22e5a9db70c7503e534dc0698', '02ee4181b05ee8346c7bc9f0fae1ffbca537617e', '0337876abebcb5395d94ebac08e7886c848686db', '03b8d0afb85033f216cd72aa549858efefceae5b', '04a96c1642b05a89d303103ccd7d14fa21c2690f', '04e1f97e7d38e496a68135bb84369a4b99418764', '05aae81910c5536362646624d7e2848d0da4e591', '075647edacac6c19201ec07311c7cef5d7d7dbf0', '0852c9305f617a71a1dde9636b51ba27aa2a86a7', '08fd0a88da386291cb802f03b209e7c8976b0747', '097381a886eba57d121c6d5905c7891c2f2a7aaa', '0a27833260260119345262d85a3ab80198a4afa9', '0a83c7012c1534604cc7062537d3915bdfaf375e', '0a93f328042751d49e0bcaa480d150865af24d66', '0ab4f182e4ccea8645c76bec65dc5ba4b43a7357', '0af3779b93780db3d81b0ac5973a746560361c97', '0c919a1776eec7387059b93a0d813c991d06c2a0', '0dc0b7fba0a5e468b94caeb862b752353ad09ffc', '0ec9e086bd44381bc0d6b5acb2ad4209d394d55d', '0eff416e1c9555ccb226f2aef31a828b5a997afe', '0f2ae4876162fe14ce7853465919987815a08e73', '106432c0ab70da382c8e5ddc1a3652babfdb602a', '107f58b19ec2195cf106781213141c7b44bf0273', '1141a6c0046a15fe2270bcc0027d48fd88179cad', '114e10d8688c6cab3afdb9a8129a054bce91f47c', '12225cf2d18f0d69674f2624105166f8322fb078', '12fa7505c09ff57fc0fe9244fc88978dd6d14885', '1314414edcc95434681dcda923ffa2424dd071dc', '1459ced43ff1ce324a6df08b1f3b0b0d44fe18b0', '1467a3e36024ee11308f29653479a5b28a63bd88', '14afbcd7ff7202010b377f8175ecb88a2ab55934', '14cb157297a4fae2389d8daf2c80e79bd2513b1f', '156cc085a63c396614c4b261756a2aa260fc77aa', '17305665de60188dc1fbe08bce8b8821abf3c8a3', '175145221eedd9bd605e3a83d293679832fa0bf9', '1929045692d5bc35779556b69d03dd385d78dbcb', '192f82cc4c9f89f7938ee4fe55e6d338e0a00356', '19dc20bccc5fd0d594937771f9cea23c082e7e73', '1a6df3e9c3b8555d3a50ff2b7c6249efb03d7241', '1afbca9f7011ec659db9cb3692cbab5b5e38499b', '1c3fb31e020c0a6b1865bc9e938324d48c879bca', '1d0d36c763297af316ba2c4a69877d5e26b12260', '1d2de46908dc938694d1981feb2e806455406388', '1d5de7a7ed362ecd596ac9ed5b85bf19d5c08ef5', '1d83c362b8995ac223a5b2d61896c0949e1268a6', '1de6d3c7f44807292653ffb9d2126d18108cd1e5', '1f90058fbc1a83c1c25e5b26e6cf5b795c0d8221', '206a2e6471187652bde2c0559465c589adc41a79', '209b30aee737ba9a13f5431123253f574f1b54d5', '20bbdea02f4f17b8a2024183b3d9474a974ca351', '2166bcf1b1bec55ef106314773315757b01deeb3', '21b851b22634e4364fad07b850065f11e3e0f852', '21d3bde93aae154f5043fb4544ee6a754c24e027', '21d601e22c028be6b98b7f79d1ca9da3a8a92d93', '220b8ff7c502c443b213e33457a38be2cf333edf', '23e1eb963f33fd59589ca28e25cc0a9d1b8b5d97', '24c95e937f6194fc05cfb9c641c403b009c97a72', '2536099c8378c8559ca936a026044a84ccb2dd27', '25a2707b0951b464922d6f1c0853e1415ad1885d', '263673dae2ba3402920d2cb77a1a0eb9418abe3b', '2668028d0040c73357831789cde8688bf487459c', '26e565a80e3d14a3fa9e9e7040a67c80c6755132', '27a7ed90ed3e9c5d2f41a9fb714824d24198da2c', '2880c6e404d399865a1a0ecadd5cc4d5cde3cd68', '28bca9440904b57d17422218215af700e0ce6561', '2a6ea724c68824d82dcee74fa17eb377c066bd2e', '2aa5001959f293203df02cc700d1d3fca7323a8f', '2b255484637009112bd313bcd3b63b22b765c4fd', '2b3719ff5a2e0a47e90a802dbf5a23e82f9fed71', '2ce09ed2cf3272be526c23704cd0a5ab9a5e75dd', '2f0ec75558afa2743237fe04763fc2ac8cd83cd2', '2f90f0ec783675427ef4cd71caaf497eca532b74', '2f9e6d9a09dcb72a263f9f7530ab83fc6e4adb58', '2fac85f069ace1522ef96226ac205ffe15b027d2', '31726787e6f1ef2c168b4434b3f1c1e5a7699974', '32f5d81c58fa264339a92d3482464d5f4c42dab2', '32f6d82496074bbd4505fbcc0bc8e1702ccc7383', '330dbb7bf19526b72a382b506ce8e0140ddb29f1', '3345dfd976e5daeb677b13650e5527d5b58efcf1', '3471aa33b3a75a0315f95a87fc2ab1a3523629a6', '3497e89b6aec7bd39790c578c14a4a303f033434', '3519d7c4c75f85ae149dd43782beb8eb43398209', '36524c02c96afd954171f206c72e28560c2c9c79', '377ea5b8dba9d4796044b79b7193a3baa283a645', '395c86e28f4b7aad55b686ac12aed0a51404b22e', '3aa4f1165bcdc2e63469ed1e07a05add057238ea', '3ab2bc8cf6544b168edb0f479a17ae72aab3ccce', '3b2286d985f4c96e476666e48dbbe83ecc7b1f1d', '3bda2fbf9489de8a1361f81fc393ec837666bd45', '3d7ea656abac66a1716402ff9c02e491dc05b3d4', '3de7143e6057046bddfa429f8a547786b7098894', '3df2406d7633784dee319c6b20b353010a753e3b', '40f45c8ee73bd272cece39c63eb6cd6b2faade1c', '41585879f0d84058a2a7bf40a2b573cf3aa1891e', '41abd327b64a76e0ef4d52937e48cadb2a24aaff', '422bb11b43e7aab755664ffeeb7df6be8f031c67', '427693ce62363c39d5ebcd495065c9e1b3379355', '428b0233112c8b5ca3ca7f0f4086ffb7f2356ce3', '42aa7dc4f635fcd72a82fab9e7ad64b1cd999767', '4359ed388f07be4911dcce32152a9d93446fc751', '43907480364462f8dd835331931089234d5fb28b', '43c79c04a946c7bd257dfd06e93887964fde9038', '43cfdd408422699dba1436ba58c344892836f2b4', '452f46980851da9d9a3fc942ea211cfacd297c7b', '464146504da333c11c4e7fe3218a6f0fcc08d696', '46bf79d2e90a611f2923ddd08ef3a363e99a29b3', '4749d574b0cecde16ec009f0579b8e7e5ad94f76', '4844f6f7a93c688d70ce5fc1d5131f9e61c8fa31', '4892ae8450736554f49e2602378b9640d92ba006', '49b6e9e79ae03b38b11da5671aeb9feae4a8e8b7', '49ea9f4e0306b02d89f35616424af2150add5b18', '4a32ffeb43d22b8c868a3bbea9d8abd08ed42010', '4a5b8a5a44d34ee471f55415cd54ac2681e686c7', '4abb28f66753c57297134fb67ba3a3f2ea17f415', '4abc37cd4eb818776709753caa4999f5d7bd06e2', '4b2229a4d97fa20ece1736b31d06922b23151935', '4cb2f596c796fda6d12248824f68460db84100e4', '4cde7eae8f31fa7c2289ae78206b2712ee25a966', '4d944a2904e43c51c2b757c25c51bb4735fb6021', '4e890f8ddd23f6645dc01ec21d05bf89590b6be9', '4fd9ca27616f1c6d78c5d2c9d7f1766d5a30475f', '5005ab4a06301e18b41c8ff27ddb2b0799c3bd7d', '50b918b7a223112c22f8e66693610835328c6162', '50c9f727c29214fe802c55ee7f1fceba2e824bed', '51060bb7db43a42315ad00cacb8bac9a1ff69632', '5109539c00753ebeec7de69a7712d94cefe942ed', '51a7d95d38fa789711eb2621ad0b44d11758d076', '51f7c0f72062e487159596793326d5b25733eb92', '538083d507d4622f28922be7f7ecf942efc47cc1', '53ae0ac3ea132bbb465c0711c9435f528b167421', '53e25cbb87a6b23c29bc1a8ed42f833a1e9279ee', '54e27ac1c25e0d9efa0d9227e93188d89bd23a73', '551037ad032fdd725024787a1979eb5ab78e944d', '556d13a3d49cec740c02486f848af107c254273e', '559b1edd11a424609964e552f03cae7bdeafbe2e', '56d36fa848ce214b71c61d5ed2ad346f70940c7c', '5734a8afd4d5b5519dd258fe76aec838da808450', '57ab323a7af88df29ab5e9e04af03cdcdf758008', '57d9f9da5a7a95cf9bf6727c7797996d1b0a998b', '57e1cfaa392a82955609994f79a536bb143ccc01', '5872522c019c0cfaf1b74da5f475177bc822b236', '59ef8a1535933b8a3c9d976538852e3a2e5259e0', '5a2892f91addeea2f4600d28b23e684be32f5b2c', '5ac58d993d5981d9afaa1b6b28af023884de0085', '5afb234424f96ea125630dfa5affd8ec05ebc37a', '5b9004063bfd5581d3992d9d2e1cb841e7b67d28', '5c1e34bb89c5ecb75b21e2ae28fcbdc1f6cfac18', '5e2328405702e00b9672992309f200cf321c8df8', '5ed6f26f052c91e733e83cd13a5cc887f0112cf9', '5f0a6cbc6c286e657e42f5827c209e8855600684', '5f7b61b62196f7e7253256382d8174e3cc2cd1e8', '5f97702ce403d1c9373687650bcbad49793c8f3b', '6026f35d6ba6dafc59c15aa2415d8139bfd8950b', '6033ae7d3c692403c6e8b1834e2589030349a6c0', '6068beaaad9d4e747e2fbee9404e2e678c0f9600', '609720d993be30578342ac1ef0987de1341deaf5', '61c53be523d14d9caf425990a243002f992bbf58', '62be2181774d9d21cc34d41c00ff2a8dca86cd8d', '62d2aa753654090ac400f771fdccb39022d74004', '62dc3fd9b164eccdf01a5a3fa1397ad35ead8ea2', '634681a6a2c349632e2da1888a697f5f0120b3f2', '63e8e5117f5b148eacb027c2f5252ef51c321e87', '653b647efe064065757b780e973cffac80403aaa', '657ff3d7f5f934a0bd348ace2e45f33250685715', '66553aed8aa409ef052b7fc75d36c2a7d8e2be5c', '68008c45a7bcfd0369d81e2f28fb62d19298f338', '682118037e2deacefdb29bb0f6611c2de55bed2c', '68da55a3e4acff01ae60b59fcd5a24ed4cde5439', '68de8b111a651c3fb5775bbdf66708f4b28082b2', '6917a3f55dbe84fcec715c98bc656dc27db3d275', '6a59be692295cc161dad855b9c3e85f8d4d6ee4f', '6b0be5fc8199da9fe7821eca3617d8977056d03e', '6c447d86239253931b0d287e1026c65821a4fc7f', '6cf3a30e6bb28b60fd16db087218b8c3f28b69f7', '6d50bd79e358ebf17d7d3a1ad3bf4c1f22fc1a45', '6eb9f22a4f8712f9accd195ea38c156cd01f8bab', '70172adeb22ffc367d129c1260f8d30b94900670', '7194267b6ce54f5d3bc61220db4c3534f120da8a', '750576e1321e30afed0671c866253198712f5c8e', '758110378ffdc4d4d068026d8241cd5e597e3bc2', '76331939e001e58e56b2aa94e03656ccc3feef13', '77d28f8643efea095b407761102541e2620ad048', '78aee64eca783cfc85db9ecae24d916f894ee718', '78e02f2d0fde865e1909ba43172fad2c4741d5cc', '79891ff8e659a4b8294af231233a00e041682de6', '7a791e657e7a1c6a4f44d787e394f11f8dd905ef', '7a93bf02bcf429211b18b014056adfba9a2046ce', '7b7ed5a3313e60c038a40c4f740ed63a142e8205', '7ba13fe59f572c4c61040330edb97039e42c3eba', '7ba8f8fb815f87e98bca62b88d756a25a72a3b1f', '7cb0857e5a207140b6ba817ca6bd446654dc56b6', '7cfb97ce9218b47fb4e4177a2feaec238bfa2d09', '7d79972f3afee640cf58aca9e6c73c79e5ce6315', '7de65787ff96a17967af71e08217815ffcffb098', '7f17c0b50bee1d4d23187422496f767a2f92f5ba', '8240e82a3cdd998fd438eb2c911ee865cdfe87ff', '82c8817f0eac82c1cf695418c09a0a26e6c8f1a7', '85a0c416d1e026e004c54361e96f898f52a072e9', '85c55a9abd38fffb041210ce7b64e6c5a5e1e92c', '8763375466df13d5dddc4a29e3e827ad358d053f', '87af22090082a4cc0d586b169849cfbfb8426fb8', '87fb8c538adb3d13a07d0c11e0aaea3be156fbcd', '8b4d265d21b80b81398638303e87b2589b6facf0', '8e35ff9ff231dc9b40b9a251a86d8b47139aa46b', '8f5f679df817a9d4a04a9d325d375644bddacb67', '8ff840a40d3f1557c55c19d4d636da77103168ce', '902add7e551461f758455900b6112087dea9324b', '906a978a0760c38ba37ebdf46cb1d8247003e91c', '90a3037d0abd198675835a5a0d458c8d45c1e008', '90c6791e382988954dad78393732fe4b2f7cf0b5', '910045d17e1fa66d1e739fa1400d3f66dcb1dd57', '911e78dcb2c6045c096697524e6876e90bc6870a', '9459ab35cd6fdaba220d5adc2f9f41224ee17416', '95bc07353373ddcdc1c438bf9f45663aa02043ba', '97045c405e22d82c7c18b8c03e77ec42b7185600', '97d3ff1688b326f1ca10a1280920de7e25176d71', '98321b4b63a8a3614c367f1a0e372fff33a54c1d', '9a631dd21d495fe27c7ea700d90cdfd0a22f2f33', '9ba3c2d4df538e34269d049f132fabcfb47c10c8', '9ba3e68cfa11fa0aa56ee07a8e07410247a5155a', '9be38fc64a3bde00cc778a04841874c614d5e09d', '9c0cabd93e046ecc2ebd75e55ffd4b2d925d3cca', '9c4dc86e605be27904d738afe11444d036d71d36', '9cb34295b4173e823115d2cd13f46dd45170eea3', '9eea73a388f7baa1d611ed934304428268c924e2', '9f5983b8389220f58dc5ee0a7e7784a7a0de62c6', 'a108c314e797c60fa8c0c909b7b029ec1b298239', 'a17cc531d1ff06b6808f4b154dd1c8c36c0b3edd', 'a1e0c292cae8769a9d922dc591aeccabbc37dd2d', 'a2aa47667e2522b56e0dd68fc9f9b7380bb23c2a', 'a2ba015310747015a2caba425a689f24b9aa0d60', 'a2f8efc1c82d0a6062232e7bc03e51658a0dd01c', 'a3ed6014e91a47d9a1b106eb5c8d129915488a2f', 'a4d4bf0410281c259bb9a38cf35242a042dcb791', 'a57a0a889e279068294151143120f371dffbed2a', 'a57a94a96894dbe123a179e263e4926755dca9b0', 'a60e443f97a33fd8615a73f5f8bc68b861a8b6c2', 'a654315c10028c56ac89b1ed0ad3c8260af2a7b6', 'a6599b149f8f3d4f75fe5ee9daad221cfdd6c029', 'a66f7de4f1572bab1d9e860c409f28d585540be0', 'a67e7305aae97481145b820977da2950b45e481b', 'a6927e97101bf1f37a48bcf823c073c2285e43b2', 'a7bd3ddfc6379e377d1a744b653e56b8c83fdbc5', 'a934bd1281e0cf02057dc5c4ad2da87e63f20757', 'a95c31592cc279f163cd81b256a0cc389a8385f0', 'aab0f5dd06f899edb2675b1019810388b2efe9c5', 'aadf4514fef1eba79d8da8166347d60404ad5408', 'ab147c78e508a3d5d250ea30b474c08289e5ced0', 'b19aa3afe16c52b4635c9236e20a296027e766b0', 'b24de7fbb1278e652ed170af17375a55f77a84cc', 'b3fe9194c02871f796040675e8c4863fac29c00f', 'b4ef74e508e6b890c19778fc9789139b534b1f6b', 'b51a15831dcba0649f1c39cbeaa7080939040075', 'b5f0ca17db81ef44bc7b585af573ce1755e0f49e', 'b6ba28a74bc508c24f7214a9460d775e65156d62', 'b89c7eecf66af04c707e0976519d22d17a9e6944', 'b8dd047246806fe48628770e1c6dd1c1a2d94db5', 'b9197c21cbbda2f5dcb4b24136b19e797f7cddb8', 'b95666cd4fd6d90e6d8ccb04d38860e0db32cef1', 'bad8470db5c7259af26e875eda168df0b8cd5c14', 'baed573c22c29cc2be11312970428379430f9b6b', 'bb39a25ecf877b06bc932c8bcb5f7748b67364bd', 'bbb21d5b23b136d6d960a1e33e8b194a3f2b65dd', 'bbd86778afce250c47a17d58019351f55590b562', 'bd8945fbab24c02e5bee6f5cf5c0e4a397789e20', 'bdb64abfa9db003213879f1057690650dd36e75b', 'be7e5d45d17dc9cd94ec108aa3e3888bc29c45d9', 'be83535338d802b8014babb62f4034c17e1ffd83', 'bf0e3ade0ecccf1fc388d0b70e92854471e42abf', 'bf169884e1570d4ab29812f5a2468fc5e198a103', 'bf761c2a2151ade2ad257a7706c88a5f8251a04e', 'bfce95f335b1da9b2ecdb9bce10e2f60335a2546', 'c00a1bbb8727233cc87f5147e8170bf951de653e', 'c162cb5fb420e2ca53de3f436992636d33ce022a', 'c1d87be1336e4c04612f8bbfdc07e4f6c8aaca1a', 'c1e1ab9125b66d4b3a6727b8b92bd324582ac6c3', 'c534d26b44d4c202ae4ab868a512b9539a5884ec', 'c57476f55a499bce900ac0719454fd5d00f03782', 'c5fbda511ff95f18d077ea845cf8dd0f5ef7694e', 'c617e5ac601c5d2cc8f01c1b5aeb36ab8607de40', 'c6fa711e03e94e641f1afa1da9e777f1de546801', 'c781ea3a77563faa6a1fe65ce8753373a2a594e9', 'c815988c2842e3358524721e627d149dc686a263', 'c94e51e7a85bdf1842aff257ec5bb392c2dbf484', 'cae17bd1a641266c8e56ccb5757d676f8d8eae5f', 'caf6ea5e25865e5134f888efa8575e8e712d7b36', 'cb43bd31380ca28115c18f09743e0ba012048a73', 'cc2b4d478bb1b48f188a4d4ff248a5274e205bdf', 'ccb238813a811b3fc48fe5a7dbc2e4b0594619ae', 'cd1a28213229db7dfef1c696bb15cb21ab0525fe', 'cebf1010d9df743b57bc220aa28a1430af1c09cd', 'cfca01c257ca6ad4003ae7b736460b8da0a649b6', 'd1b477b338f038b43b9d4b52fda1e1f10d6ebfcf', 'd36987e1b8e7e4ef06b205a870ca1746e3636bc2', 'd37246f93296138bfea8aee1e9e2d7863e33c9f8', 'd3bbb8ddc6779879234516a8302bf221613a05a9']
        common_query = common_query__builder(start=start, size=size,
                                             source=["paperId","doi","authors","fieldsOfStudy","title","topics"],
                                             return_top_author=True, top_author_size=10,
                                             return_fos_aggs=True,
                                             return_venue_aggs=True)
        query = {
            "query": {
                "bool": {
                    "should": []
                }
            }
        }
        for pid in paper_ids:
            query["query"]["bool"]["should"].append({
                "match": {
                    "paperId.keyword": pid
                }
            })

        query.update(common_query)
        print('get_some_citations_2 query: ', query)
        result = es.search(index=index, body=query)
        #print('get_some_citations_2 result: ', result)

    except NotFoundError:
        print('paper {} not found'.format(paper_id))
        return {}


if __name__ == "__main__":
    get_some_citations_2(elasticsearch_connection, "paper_test", "12345")