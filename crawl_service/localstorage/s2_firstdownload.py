from shared_utilities.utilities import grouper, store_gz
from crawl_service.helpers.s2page_parser import crawl_base_sitemap, crawl_second_sitemap, get_paper_api_v2, \
    extract_url_id, get_pdf_link_and_name
from constants.constants import PAPER_METADATA_PATH

import concurrent.futures
from tqdm import tqdm
import itertools
import re
import os


def downloader(paper_url, paper_sitemap):
    paper_id = extract_url_id(paper_url)
    sitemap_id = re.findall("\d+", paper_sitemap)[0]
    s2paper = get_paper_api_v2(paper_id, sitemap_id)
    try:
        paper_document = {
            "paperId": s2paper["paperId"],
            "corpusId": s2paper["corpusId"],
            "title": s2paper["title"],
            "abstract": s2paper["abstract"],
            "venue": s2paper["venue"],
            "year": s2paper["year"],
            "citationVelocity": s2paper["citationVelocity"],
            "doi": s2paper["doi"],
            "influentialCitationCount": s2paper["influentialCitationCount"],
            "citations_count": len(s2paper["citations"]),
            "topics": [{"topic": topic["topic"],
                        "topicId": topic["topicId"]}
                       for topic in s2paper["topics"]],
            "references_count": len(s2paper["references"]),
            "pdf_url": get_pdf_link_and_name(paper_url, sitemap_id)[0],
            "fieldsOfStudy": s2paper["fieldsOfStudy"],
            "citations": [{"paperId": citation["paperId"],
                           "isInfluential": citation["isInfluential"],
                           "intent": citation["intent"]}
                          for citation in s2paper["citations"]],
            "references": [{"paperId": reference["paperId"],
                            "intent": reference["intent"],
                            "isInfluential": reference["isInfluential"]}
                           for reference in s2paper["references"]],
            "authors": [{"authorId": author["authorId"],
                         "name": author["name"]} for author in s2paper["authors"]]
        }

        store_gz(paper_document, "{}/sitemap_{}/paper_{}.json.gz".format(PAPER_METADATA_PATH, sitemap_id, paper_id))
        return paper_id
    except Exception as e:
        print("paper {} DOWNLOAD error: {}".format(paper_id, e))


# def download_data(start, end=None):
#     paper_sitemaps_list = crawl_base_sitemap("https://www.semanticscholar.org/sitemap_paper_index.xml")
#     paper_sitemaps = paper_sitemaps_list[start:] if end is None else paper_sitemaps_list[start:end]
#     with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
#         for paper_sitemap in paper_sitemaps:
#             paper_urls = crawl_second_sitemap(paper_sitemap)[:50]
#             if paper_urls is not None:
#                 urls_grouper = grouper(paper_urls, 1000)
#                 for index, urls_group in enumerate(urls_grouper):
#                     future_to_url = {executor.submit(downloader, paper_url,paper_sitemap): paper_url for paper_url in urls_group if
#                                              paper_url is not None}
#                     # Just ignore this
#                     pbar = tqdm(concurrent.futures.as_completed(future_to_url), total=len(future_to_url), unit="paper")
#                     for future in pbar:
#                         pbar.set_description("Paper_sitemap_{}_group_({}/5)".format(re.findall("\d+", paper_sitemap)[0],index))
#                         paper_url = future_to_url[future]
#                         try:
#                             paper_id = future.result()
#                         except Exception as exc:
#                             print('%r generated an exception: %s' % (paper_url, exc))


def download_data(start, end=None):
    paper_sitemaps_list = crawl_base_sitemap("https://www.semanticscholar.org/sitemap_paper_index.xml")
    paper_sitemaps = iter(paper_sitemaps_list[start:]) if end is None else iter(paper_sitemaps_list[start:end])

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = {
            executor.submit(crawl_second_sitemap, paper_sitemap): paper_sitemap
            for paper_sitemap in itertools.islice(paper_sitemaps, 6)
        }
        while futures:
            # Wait for the next future to complete.
            done, notdone = concurrent.futures.wait(
                futures, return_when=concurrent.futures.FIRST_COMPLETED
            )
            print(list(os.listdir(PAPER_METADATA_PATH)))
            print("Done: ", [dfut.result()[1] for dfut in done])
            print("Not done: ", [ndfut.result()[1] for ndfut in notdone])
            print("Before pop: ", len(futures))
            for fut in done:
                try:
                    futures.pop(fut)
                    print(f"The outcome is {fut.result()[1]} {len(fut.result()[0])}")
                except Exception as e:
                    print(f"{fut.result()[1]} causes error {e}")
            print("After pop: ", len(futures))
            ##Do not print itertools.islice(paper_sitemaps, len(done))
            # Schedule the next set of futures.  We don't want more than N futures
            # in the pool at a time, to keep memory consumption down.
            for paper_sitemap in itertools.islice(paper_sitemaps, len(done)):
                fut = executor.submit(crawl_second_sitemap, paper_sitemap)
                futures[fut] = paper_sitemap

            print("After add: ", len(futures))
            print("\n")


if __name__ == '__main__':
    download_data(10, 20)
