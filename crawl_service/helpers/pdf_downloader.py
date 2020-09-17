from shared_utilities.utilities import load_url

BASE_SCIHUB = "https://scihub.wikicn.top/"


def download(pdf_url):
    res = load_url(BASE_SCIHUB+pdf_url, proxy=True, return_content=True)
    print(res)


if __name__ == '__main__':
    download("https://doi.org/10.2307/1058333")
