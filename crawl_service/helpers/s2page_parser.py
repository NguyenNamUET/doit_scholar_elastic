import re
from bs4 import BeautifulSoup
from crawl_service.helpers.utilities import load_url, read_text, write_to_record

#paraphrase ssh: nguyennam
#https://scihub.wikicn.top/
def get_pdf_link_and_name(url):
    try:
        soup = load_url(url, return_content=True, proxy=True)

        source = soup.find("a", {"class":"icon-button button--full-width button--primary"})
        pdf_link = None
        if source is not None:
            pdf_link = source["href"]

        alternate_source = soup.find("a", {"class":"icon-button alternate-source-link-button"})
        if alternate_source is not None:
            pdf_link = alternate_source["link"]

        # if re.search("pdf", pdf_link, flags=re.IGNORECASE) is not None:
        #     print(pdf_link)
        #     write_to_record(pdf_link, "test/pdf.txt", by_line=True, is_append=True)
        # else:
        #     write_to_record(pdf_link+"\n"+url+"\n", "test/not_pdf.txt", by_line=True, is_append=True)

        pdf_name = soup.find("meta", {"name":"citation_title"})["content"]

        return pdf_link, pdf_name
    except Exception as e:
        print("{} caused PDF error {}".format(url, e))
        return None


def get_paper_api(corpusID):
    paper = load_url("https://api.semanticscholar.org/v1/paper/CorpusID:{}".format(corpusID),
                     proxy=True, return_json=True)
    return paper


def get_paper_api_v2(paperID):
    paper = load_url("https://api.semanticscholar.org/v1/paper/{}".format(paperID),
                     return_json=True, proxy=True)
    return paper


def extract_url_id(paper_url):
    find_slash = list(re.finditer("\/", paper_url))[-1]
    return paper_url[find_slash.span()[1]:]


def crawl_base_sitemap(base_sitemap):
    base_sitemap_soup = load_url(base_sitemap, return_content=True, proxy=True)
    all_sitemaps_soup = base_sitemap_soup.find_all("loc")
    return [sitemap.text for sitemap in all_sitemaps_soup]


def crawl_second_sitemap(sitemap_url):
    try:
        sitemap_content = load_url(sitemap_url, return_content=True, proxy=True)
        all_paper_urls_soup = sitemap_content.find_all("loc")
        return [sitemap.text for sitemap in all_paper_urls_soup]
    except Exception as e:
        return None


if __name__ == '__main__':
    print(get_pdf_link_and_name("https://www.semanticscholar.org/paper/Studies-of-the-Mortality-of-Atomic-Bomb-Survivors%2C-Ozasa-Shimizu/6c8427dcc45312d46396defb9f2e15dd36ddd110"))