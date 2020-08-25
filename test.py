import requests
import concurrent.futures
import re
from bs4 import BeautifulSoup
from es_service.es_helpers.es_connection import elasticsearch_connection
from es_service.es_helpers.es_operator import insert_doc
from es_service.es_helpers.utilites import write_to_record

ID_MAPPING_DOCUMENT_INDEX = "idmapping"


def load_url(url, return_content=False, proxy=False, return_json=False):
    headers = {"User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:79.0) Gecko/20100101 Firefox/79.0",
               "Connection": "keep-alive",
               "Accept-Language": "en-US,en;q=0.5"}
    try:
        if proxy:  ##Do not reveal this proxy
            proxies = {
                "https": "https://lum-customer-hl_26f509b3-zone-static:emgsedqdj28n@zproxy.lum-superproxy.io:22225"
            }
            response = requests.get(url, headers=headers, proxies=proxies)

        else:
            response = requests.get(url)

        if return_content:
            soup = BeautifulSoup(response.content, "html.parser")
            return soup
        elif return_json:
            return response.json()
        else:
            return response
    except Exception as e:
        print("load_url() error: ", e)


def crawl_base_sitemap():
    base_sitemap = "https://www.semanticscholar.org/sitemap_paper_index.xml"
    base_sitemap_soup = load_url(base_sitemap, return_content=True, proxy=True)
    all_sitemaps_soup = base_sitemap_soup.find_all("loc")
    return [sitemap.text for sitemap in all_sitemaps_soup]


def crawl_papers_sitemap(sitemap_url):
    sitemap_content = load_url(sitemap_url, return_content=True, proxy=True)
    all_paper_urls_soup = sitemap_content.find_all("loc")
    return [sitemap.text for sitemap in all_paper_urls_soup]


def get_corpus_id_from_paper_url(paper_url):
    soup = load_url(paper_url, return_content=True, proxy=True)
    corpus_span = soup.find_all("span", {"data-selenium-selector": "corpus-id"})[0]

    return re.findall("\d+", corpus_span.text)[0]


def get_paper_id_from_paper_url(paper_url):
    find_slash = list(re.finditer("\/", paper_url))[-1]
    return paper_url[find_slash.span()[1]:]


def index_mapping_document(paper_url):
    corpusID = get_corpus_id_from_paper_url(paper_url)
    paperID = get_paper_id_from_paper_url(paper_url)

    mapping = {"paperID":paperID, "corpusID":corpusID}
    insert_doc(elasticsearch_connection, ID_MAPPING_DOCUMENT_INDEX, corpusID, mapping)
    print("Successful")
    return corpusID


def index_mapping(papers_sitemap):
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        # Start the load operations and mark each future with its URL
        future_id = {executor.submit(index_mapping_document, psm): psm for psm in papers_sitemap}
        for future in concurrent.futures.as_completed(future_id):
            psm = future_id[future]
            try:
                mapping = future.result()
            except Exception as exc:
                print("%r generated an exception: %s" % (psm, exc))
                write_to_record(psm, "/home/nguyennam/Downloads/doit_scholar_elastic/failed_sitemaps.txt",
                                is_append=True, by_line=True)


if __name__ == "__main__":
    paper_base_sitemap_list = crawl_base_sitemap()
    for paper_base_sitemap in paper_base_sitemap_list[9:]:
        papers_sitemap = crawl_papers_sitemap(paper_base_sitemap)
        index_mapping(papers_sitemap)
    # from es_service.es_helpers.utilites import read_text
    # path_1 = "/home/nguyennam/Downloads/collected_paperID.txt"
    # path_2 = "/home/nguyennam/Downloads/papers"
    # path_3 = "/home/nguyennam/Downloads/authors"
    # paperIDs = read_text(path_1)
    # paperURLs = ["https://api.semanticscholar.org/{}".format(id) for id in paperIDs]
    # index_mapping(paperURLs)

