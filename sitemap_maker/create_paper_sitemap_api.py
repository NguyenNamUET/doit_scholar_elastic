import os
import datetime
from jinja2 import Template
import re
from shared_utilities.utilities import store_gz, load_jsonl_from_gz
from constants.constants import STORAGE_PATH
from datetime import datetime

HOST_NAME = "http://localhost:3000"

def make_sitemap_paper_title(title):
    sub1 = re.sub("[\(|\[|{]([^)]*)[\)|\]|}]","",title.strip())
    sub2 = re.sub("\s+|\W+","-",sub1)
    sub3 = re.sub("\-{2,}","-",sub2)
    sub4 = re.sub("\-$","",sub3)

    return sub4


def make_paper_sitemap():
    sitemap_obj = {
        "path": "/paper_sitemap_index.xml",
        "hostname": HOST_NAME,
        "sitemaps": []
      }
    for sitemap in os.listdir(STORAGE_PATH):
        loc = sitemap.replace("_","-paper-")
        sitemap_index_obj = {
            'path': f"/{loc}.xml",
            'lastmod': str(datetime.now().strftime("%Y-%m-%d")),
            'exclude': ["/search", "/vi/search", '/', '/vi'],
            'routes': []
        }
        for paper_file in os.listdir((os.path.join(STORAGE_PATH, sitemap)))[:2]:
            if re.search(".json.gz", paper_file) is not None:
                paper = load_jsonl_from_gz(os.path.join(STORAGE_PATH, sitemap, paper_file))
                title = make_sitemap_paper_title(paper["title"])
                id = paper["paperId"]
                paper_sitemap_obj = {
                    'url': f"/paper/{title}.p-{id}",
                    'i18n': True,
                    'priority': 0.8,
                    'lastmod': str(datetime.now().strftime("%Y-%m-%d"))
                }
                sitemap_index_obj["routes"].append(paper_sitemap_obj)

        sitemap_obj["sitemaps"].append(sitemap_index_obj)

    print("COMPLETE")
    return sitemap_obj


if __name__ == '__main__':
    print(os.listdir("/"))