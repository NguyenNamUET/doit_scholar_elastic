from crawl_service.helpers.utilities import grouper, store_gz
from crawl_service.helpers.s2page_parser import crawl_base_sitemap, crawl_second_sitemap, get_paper_api_v2, extract_url_id, get_pdf_link_and_name

import concurrent.futures


def downloader(paper_url):
    paper_id = extract_url_id(paper_url)
    paper_document = get_paper_api_v2(paper_id)
    try:
        paper_document["citations_count"] = len(paper_document["citations"])
        paper_document["references_count"] = len(paper_document["references"])
        paper_document["pdf_url"] = get_pdf_link_and_name(paper_url)[0]

        store_gz(paper_document, "test/paper_{}.json.gz".format(paper_id))
        return paper_id
    except Exception as e:
        print("paper {} DOWNLOAD error: {}".format(paper_id, e))


def download_data():
    paper_sitemaps_list = crawl_base_sitemap("https://www.semanticscholar.org/sitemap_paper_index.xml")
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        for paper_sitemap in paper_sitemaps_list[:1]:
            paper_urls = crawl_second_sitemap(paper_sitemap)
            if paper_urls is not None:
                for index, urls_group in enumerate(grouper(paper_urls, 1000)):
                    future_to_url = {executor.submit(downloader, paper_url): paper_url for paper_url in urls_group if paper_url is not None}
                    #Just ignore this
                    for future in concurrent.futures.as_completed(future_to_url):
                        paper_url = future_to_url[future]
                        try:
                            paper_id = future.result()
                        except Exception as exc:
                            print('%r generated an exception: %s' % (paper_url, exc))


if __name__ == '__main__':
    download_data()