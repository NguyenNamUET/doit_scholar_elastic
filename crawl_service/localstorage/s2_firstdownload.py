from shared_utilities.utilities import grouper, store_gz
from crawl_service.helpers.s2page_parser import crawl_base_sitemap, crawl_second_sitemap, get_paper_api_v2, \
    extract_url_id, get_pdf_link_and_name
from constants.constants import PAPER_METADATA_PATH

import concurrent.futures
from tqdm import tqdm
import re


def downloader(paper_url, paper_sitemap):
    paper_id = extract_url_id(paper_url)
    sitemap_id = re.findall("\d+", paper_sitemap)[0]
    try:
        s2paper = get_paper_api_v2(paper_id, sitemap_id)

        paper_document = {
            "paperId": s2paper["paperId"],
            "corpusId": s2paper["corpusId"],
            "title": s2paper["title"],
            "abstract": s2paper["abstract"],
            "venue": s2paper["venue"],
            "year": s2paper["year"] if s2paper["year"]=="" else 0,
            "citationVelocity": s2paper["citationVelocity"],
            "doi": s2paper["doi"],
            "influentialCitationCount": s2paper["influentialCitationCount"],
            "topics": [{"topic": topic["topic"],
                        "topicId": topic["topicId"],
                        "topic_name__id": topic["topic"]+"___"+topic["topicId"]}
                       for topic in s2paper["topics"]],
            "pdf_url": get_pdf_link_and_name(paper_url, sitemap_id)[0],
            "fieldsOfStudy": s2paper["fieldsOfStudy"],
            "citations": [{"paperId": citation["paperId"],
                           "isInfluential": citation["isInfluential"],
                           "intent": citation["intent"],
                           "year": citation["year"],
                           "venue": citation["venue"],
                           "title": citation["title"],
                           "doi": citation["doi"]
                           }
                          for citation in s2paper["citations"]],
            #"citations_count": len(s2paper["citations"]),
            "references": [{"paperId": reference["paperId"],
                           "isInfluential": reference["isInfluential"],
                           "intent": reference["intent"],
                           "year": reference["year"],
                           "venue": reference["venue"],
                           "title": reference["title"],
                           "doi": reference["doi"]
                           }
                           for reference in s2paper["references"]],
            #"references_count": len(s2paper["references"]),
            "authors": [{"authorId": author["authorId"],
                         "name": author["name"],
                         "author_name__id": author["name"]+"___"+author["authorId"]
                         } for author in s2paper["authors"]],
            "authors_count": len(s2paper["authors"])
        }

        store_gz(paper_document, f"{PAPER_METADATA_PATH}/sitemap_{sitemap_id}/paper_{paper_id}.json.gz")
        return paper_id
    except Exception as e:
        print(f"paper {paper_id} DOWNLOAD error: {e}")


def download_data(start, end=None):
    paper_sitemaps_list = crawl_base_sitemap("https://www.semanticscholar.org/sitemap_paper_index.xml")
    paper_sitemaps = paper_sitemaps_list[start:] if end is None else paper_sitemaps_list[start:end]
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        for paper_sitemap in paper_sitemaps:
            paper_urls = crawl_second_sitemap(paper_sitemap)[:10]
            if paper_urls is not None:
                urls_grouper = grouper(paper_urls, 10)
                for index, urls_group in enumerate(urls_grouper):
                    future_to_url = {executor.submit(downloader, paper_url,paper_sitemap): paper_url for paper_url in urls_group if
                                             paper_url is not None}
                    # Just ignore this
                    pbar = tqdm(concurrent.futures.as_completed(future_to_url), total=len(future_to_url), unit="paper")
                    for future in pbar:
                        pbar.set_description("Paper_sitemap_{}_group_({}/3100)".format(re.findall("\d+", paper_sitemap)[0],index))
                        paper_url = future_to_url[future]
                        try:
                            paper_id = future.result()
                        except Exception as exc:
                            print('%r generated an exception: %s' % (paper_url, exc))



if __name__ == '__main__':
    # import requests
    # from constants.constants import HEADERS, PROXY
    # proxies = {
    #     "http": PROXY
    # }
    # res = requests.get("https://api.semanticscholar.org/v1/paper/91aad3a56e1b5fb5b641b31e7e801948d5b267af", headers=HEADERS, proxies=proxies)
    # print(res.json())
    # import os
    # from shared_utilities.utilities import load_jsonl_from_gz
    # for file in os.listdir("/home/nguyennam/Downloads/data/20201113_222438/sitemap_0000000"):
    #     paper = load_jsonl_from_gz("/home/nguyennam/Downloads/data/20201113_222438/sitemap_0000000/"+file)
    #     if "paperId" not in paper.keys():
    #         print(file, "/////", paper)
    download_data(0)