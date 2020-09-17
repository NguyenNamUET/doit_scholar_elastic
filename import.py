from crawl_service.localstorage.s2_downloader import download_data
from es_service.es_index.es_papers_index import index_papers

if __name__ == '__main__':
    download_data()
    index_papers()
