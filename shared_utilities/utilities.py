import json
import fasteners
import os
import gzip
import requests
from bs4 import BeautifulSoup
from itertools import zip_longest


def grouper(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)


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
    print(list(grouper([1,2,3,4,5,6,7,8,9],3)))