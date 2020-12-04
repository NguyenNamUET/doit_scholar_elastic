import os
import datetime
from jinja2 import Template
import re
from shared_utilities.utilities import store_gz, load_jsonl_from_gz

PATH = "/home/nguyennam/Downloads/data/20201204_112950"
STORAGE_PATH = "/home/nguyennam/Downloads/data/sitemaps"


def make_sitemap_paper_title(title):
    sub1 = re.sub("[\(|\[|{]([^)]*)[\)|\]|}]","",title.strip())
    sub2 = re.sub("\s+|\W+","-",sub1)
    sub3 = re.sub("\-{2,}","-",sub2)
    sub4 = re.sub("\-$","",sub3)

    return sub4


def make_base_sitemap(local_stored=False):
    base_sitemaps = {"pages":[], "changefreq":"monthly"}
    for sitemap in os.listdir(PATH):
        loc = sitemap.replace("_","-paper-")
        base_sitemaps["pages"].append(f"https://compasify/{loc}.xml.gz")

    sitemap_template = '''<?xml version="1.0" encoding="UTF-8"?>
    <sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
        {% for page in pages %}
        <sitemap>
            <loc>{{page}}</loc>
            <changefreq>{{changefreq}}</changefreq>    
        </sitemap>
        {% endfor %}
    </sitemapindex>'''

    template = Template(sitemap_template)
    sitemap_output = template.render(base_sitemaps)
    if local_stored:
    # Write the File to Your Working Folder
        store_gz(sitemap_output, f"{STORAGE_PATH}/base_sitemap/sitemap_paper_index.xml.gz")

    return sitemap_output


def make_sitemap_xml(sitemap_path, local_stored=False):
    base_sitemaps = {"pages": [], "changefreq": "weekly", "priority":0.8}
    for sitemap in os.listdir(os.path.join(PATH, sitemap_path)):
        if re.search(".json.gz", sitemap) is not None:
            paper = load_jsonl_from_gz(os.path.join(PATH, sitemap_path, sitemap))
            title = make_sitemap_paper_title(paper["title"])
            id = paper["paperId"]
            lastmod_date = datetime.datetime.now().strftime('%Y-%m-%d')
            base_sitemaps["pages"].append((f"https://compasify/{title}.p-{id}", lastmod_date))

    sitemap_template = '''<?xml version="1.0" encoding="UTF-8"?>
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
            {% for page in pages %}
            <url>
                <loc>{{page[0]}}</loc>
                <lastmod>{{page[1]}}</lastmod>
                <priority>{{priority}}</priority>
                <changefreq>{{changefreq}}</changefreq>    
            </url>
            {% endfor %}
        </urlset>'''

    template = Template(sitemap_template)
    sitemap_output = template.render(base_sitemaps)
    # Write the File to Your Working Folder
    if local_stored:
        id = re.findall("\d+", sitemap_path)[0]
        store_gz(sitemap_output, f"{STORAGE_PATH}/paper_sitemaps/sitemap-paper-{id}.xml.gz")

    return sitemap_output

if __name__ == '__main__':
    make_base_sitemap(True)