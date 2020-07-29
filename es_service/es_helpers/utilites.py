import json


# def load_jsonl(path):
#     with open(path, "r") as json_file:
#         json_objects = [json.loads(jsonline) for jsonline in json_file.readlines()]
#
#     return json_objects


def load_json(json_path):
    try:
        with open(json_path, "r", encoding='utf8') as json_file:
            return json.load(json_file)
    except Exception as e:
        print("load_json({}): {}".format(json_path, e))
