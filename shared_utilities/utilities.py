import json
import fasteners
import os
import gzip
import requests
from bs4 import BeautifulSoup
from itertools import zip_longest
from constants.constants import HEADERS, PROXY


def grouper(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)


def load_url(url, error_path, return_content=False, proxy=False, return_json=False):
    try:
        if proxy:  ##Do not reveal this proxy
            proxies = {
                "http": PROXY
            }
            response = requests.get(url, headers=HEADERS, proxies=proxies)
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
        write_to_record(url, error_path, by_line=True, is_append=True)


# async def load_url_async(url, session, return_content=False, proxy=False, return_json=False):
#     try:
#         headers = {"User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:79.0) Gecko/20100101 Firefox/79.0",
#                    "Connection": "keep-alive",
#                    "Accept-Language": "en-US,en;q=0.5"}
#         if proxy:  ##Do not reveal this proxy
#             proxies = "https://lum-customer-hl_26f509b3-zone-static:emgsedqdj28n@zproxy.lum-superproxy.io:22225"
#
#             async with session.get(url, headers=headers, proxy=proxies) as response:
#                 if return_content:
#                     soup = BeautifulSoup(await response.text(), "html.parser")
#                     return soup
#                 elif return_json:
#                     return await response.json()
#                 else:
#                     return response
#         else:
#             async with session.get(url) as response:
#                 if return_content:
#                     soup = BeautifulSoup(await response.text(), "html.parser")
#                     return soup
#                 elif return_json:
#                     return await response.json()
#                 else:
#                     return response
#
#     except Exception as e:
#         print("load_url_async() error: ", e)
#         write_to_record(url,
#                         "/home/nguyennam/Downloads/doit_scholar_elastic/failed_url.txt", by_line=True,
#                         is_append=True)


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


def store_gz(json_obj, file_output_path, is_append=False):
    os.makedirs(os.path.dirname(file_output_path), exist_ok=True)
    with fasteners.InterProcessLock(file_output_path):
        if is_append:
            with gzip.open(file_output_path, 'ab') as f:
                f.write(('\n' + json.dumps(json_obj, ensure_ascii=False, indent=2)).encode('utf-8'))
        else:
            with gzip.open(file_output_path, 'wb') as f:
                f.write((json.dumps(json_obj, ensure_ascii=False, indent=2)).encode('utf-8'))


def load_jsonl_from_gz(file_gz_path):
    try:
        with gzip.open(file_gz_path, 'rt') as f:
            file_content = f.read()
            obj = json.loads(file_content)
            return obj
    except Exception as e:
        print("load_jsonl_from_gz {} error {}".format(file_gz_path, e))


if __name__ == '__main__':
    import requests
    #https://lum-customer-hl_26f509b3-zone-static:emgsedqdj28n@zproxy.lum-superproxy.io:22225
    #curl --proxy zproxy.lum-superproxy.io:22225 --proxy-user lum-customer-hl_26f509b3-zone-static:emgsedqdj28n "http://lumtest.com/myip.json"
    proxies = {
        'http': "http://service_8798:45f30e69ee@rotating.proxy-spider.com:1500"
    }

    # Create the session and set the proxies.
    s = requests.Session()
    s.proxies = proxies

    # Make the HTTP request through the session.
    r = s.get('http://ip-api.com/')

    # Check if the proxy was indeed used (the text should contain the proxy IP).
    print(r.text)
