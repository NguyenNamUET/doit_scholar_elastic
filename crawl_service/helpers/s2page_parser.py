import re
from shared_utilities.utilities import load_url, write_to_record
from constants.constants import PAPER_METADATA_PATH


def get_pdf_link_and_name(paper_url, sitemap_id):
    try:
        soup = load_url(paper_url,
                        error_path="{}/sitemap_{}/pdf_error.txt".format(PAPER_METADATA_PATH,sitemap_id),
                        return_content=True, proxy=True)

        source = soup.find("a", {"class":"icon-button button--full-width button--primary"})
        pdf_link = None
        if source is not None:
            pdf_link = source["href"]

        alternate_source = soup.find("a", {"class":"icon-button alternate-source-link-button"})
        if alternate_source is not None:
            pdf_link = alternate_source["link"]

        pdf_name = soup.find("meta", {"name":"citation_title"})["content"]

        return pdf_link, pdf_name
    except Exception as e:
        print("{} caused PDF error {}".format(paper_url, e))
        return None


def get_paper_api_v2(paperID, sitemap_id):
    paper = load_url("https://api.semanticscholar.org/v1/paper/{}".format(paperID),
                     error_path="{}/sitemap_{}/paper_error.txt".format(PAPER_METADATA_PATH,sitemap_id),
                     return_json=True, proxy=True)
    return paper


def get_paper_api(corpusID):
    paper = load_url("https://api.semanticscholar.org/v1/paper/CorpusID:{}".format(corpusID),
                     proxy=True, return_json=True)
    return paper


def extract_url_id(paper_url):
    find_slash = list(re.finditer("\/", paper_url))[-1]
    return paper_url[find_slash.span()[1]:]


def crawl_base_sitemap(base_sitemap):
    base_sitemap_soup = load_url(base_sitemap,
                                 error_path="{}/base_sitemap.txt".format(PAPER_METADATA_PATH),
                                 return_content=True, proxy=True)
    all_sitemaps_soup = base_sitemap_soup.find_all("loc")
    return [sitemap.text for sitemap in all_sitemaps_soup]


def crawl_second_sitemap(sitemap_url):
    sitemap_id = re.findall("\d+", sitemap_url)[0]
    try:
        sitemap_content = load_url(sitemap_url,
                                   error_path="{}/sitemap_{}/sitemap_error.txt".format(PAPER_METADATA_PATH,sitemap_id),
                                   return_content=True, proxy=True)
        all_paper_urls_soup = sitemap_content.find_all("loc")
        write_to_record(sitemap_url, "{}/sitemap_{}/sitemap_error.txt".format(PAPER_METADATA_PATH,sitemap_id),
                        by_line=True, is_append=True)
        return [sitemap.text for sitemap in all_paper_urls_soup], sitemap_url
    except Exception as e:
        print("Sitemap {} caused error {}".format(sitemap_id, e))


if __name__ == '__main__':
    print(get_pdf_link_and_name("https://www.semanticscholar.org/paper/%E5%9D%9A%E6%8C%81%E5%85%AC%E6%AD%A3%E5%8F%B8%E6%B3%95-%E5%BB%BA%E8%AE%BE%E2%80%9C%E5%B9%B3%E5%AE%89%E9%87%8D%E5%BA%86%E2%80%9D-%E7%8E%8B%E4%B8%AD%E4%BC%9F/f70525a68d6554de2e8b02eb68854f60744dd83c",
                                "000001"))