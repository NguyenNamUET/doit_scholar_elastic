from datetime import datetime

VERSION = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
FILE_VERSION = str(VERSION)[:10].replace("-", "") + "_" + str(VERSION)[11:19].replace(":", "")

BASE_SCIHUB = "https://scihub.wikicn.top/"
PAPER_METADATA_PATH = "/home/nguyennam/Downloads/data/{}".format(FILE_VERSION) #/storage/dataStorage/{}



#passphrase ssh: nguyennam
if __name__ == '__main__':
    print(FILE_VERSION)