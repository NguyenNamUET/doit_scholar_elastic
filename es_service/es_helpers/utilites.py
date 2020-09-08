import json
import fasteners
import os
import re
import requests
from bs4 import BeautifulSoup
from itertools import zip_longest


def grouper(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)


# def load_jsonl(path):
#     with open(path, "r") as json_file:
#         json_objects = [json.loads(jsonline) for jsonline in json_file.readlines()]
#
#     return json_objects


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
        write_to_record(url,
                        "/home/nguyennam/Downloads/doit_scholar_elastic/failed_url.txt", by_line=True,
                        is_append=True)


def write_to_record(object, file_output_path, by_line=False, is_append=False):
    os.makedirs(os.path.dirname(file_output_path), exist_ok=True)
    with fasteners.InterProcessLock(file_output_path):
        try:
            if not is_append:
                with open(file_output_path, "w+") as file:
                    if not by_line:
                        file.write(object)
                    else:
                        file.write(object + '\n')

            else:
                with open(file_output_path, "a") as file:
                    if not by_line:
                        file.write(object)
                    else:
                        file.write(object + '\n')
        except Exception as e:
            print("write_to_record error: ", e)


def load_json(json_path):
    try:
        with open(json_path, "r", encoding='utf8') as json_file:
            return json.load(json_file)
    except Exception as e:
        print("load_json({}): {}".format(json_path, e))


def read_text(file_path):
    contents = []
    with open(file_path, "r") as txt_file:
        for line in txt_file.readlines():
            contents.append(line.replace("\n", ""))

    return contents


def extract_url_id(paper_url):
    find_slash = list(re.finditer("\/", paper_url))[-1]
    return paper_url[find_slash.span()[1]:]


def get_paper_api(corpusID):
    paper = load_url("https://api.semanticscholar.org/v1/paper/CorpusID:{}".format(corpusID),
                     proxy=True, return_json=True)
    return paper


def get_paper_api_v2(paperID):
    paper = load_url("https://api.semanticscholar.org/v1/paper/{}".format(paperID),
                     return_json=True, proxy=True)
    return paper


def get_author_api(authorId):
    author = load_url("https://api.semanticscholar.org/v1/author/{}".format(authorId),
                     return_json=True, proxy=True)
    return author


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