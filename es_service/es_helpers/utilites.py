import json
import fasteners
import os
# def load_jsonl(path):
#     with open(path, "r") as json_file:
#         json_objects = [json.loads(jsonline) for jsonline in json_file.readlines()]
#
#     return json_objects


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
