from es_service.es_helpers.es_connection import elasticsearch_connection
from es_service.es_helpers.es_operator import insert_doc
from es_service.es_constant.constants import PAPER_DOCUMENT_INDEX
from constants.constants import PAPER_METADATA_PATH
from shared_utilities.utilities import load_jsonl_from_gz, grouper

import os
import concurrent.futures
from tqdm import tqdm
import math


def index_paper_document(path):
    try:
        paper_document = load_jsonl_from_gz(PAPER_METADATA_PATH+"/"+path)
        insert_doc(es=elasticsearch_connection, index=PAPER_DOCUMENT_INDEX,
                   id=paper_document["paperId"], body=paper_document, verbose=False)
        return path
    except Exception as e:
        print("Paper {} index error: {}".format(path, e))


def index_papers():
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        dir_group = grouper(os.listdir(PAPER_METADATA_PATH), 1000)
        for index, paths_group in enumerate(dir_group):
            future_to_path = {executor.submit(index_paper_document, path): path for path in paths_group if
                                    path is not None}
            # Just ignore this
            pbar = tqdm(concurrent.futures.as_completed(future_to_path), total=len(future_to_path), unit="paper")
            for future in pbar:
                pbar.set_description("Paper index({}/{})".format(index, math.ceil(len(list(dir_group))/1000)))
                paper_path = future_to_path[future]
                try:
                    paper_id = future.result()
                except Exception as exc:
                    print('%r generated an exception: %s' % (paper_path, exc))

