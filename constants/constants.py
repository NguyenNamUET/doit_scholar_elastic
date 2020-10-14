from datetime import datetime

VERSION = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
FILE_VERSION = str(VERSION)[:10].replace("-", "") + "_" + str(VERSION)[11:19].replace(":", "")

BASE_SCIHUB = "https://scihub.wikicn.top/"
PAPER_METADATA_PATH = "/home/nguyennam/Downloads/data/{}".format(FILE_VERSION)  # /storage/dataStorage/{}

PROXY = "http://service_8798:45f30e69ee@rotating.proxy-spider.com:1500/"
HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:81.0) Gecko/20100101 Firefox/81.0",
           "Connection": "keep-alive"
           }
# headers = {"User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:79.0) Gecko/20100101 Firefox/79.0",
#                "Connection": "keep-alive",
#                "Accept-Language": "en-US,en;q=0.5"}

# passphrase ssh: nguyennam
