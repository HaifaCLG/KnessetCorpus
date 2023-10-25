import re
from multiprocessing import Pool, Lock

from conllu import parse_incr, parse
from aux_functions import *
MULTIPROCESSING = True
path_to_conllu_files = committee_protocols_conllu_files_path
# path_to_conllu_files = plenary_protocols_conllu_files_path
path_to_processed_protocols = committees_processed_jsonl_protocols_files
# path_to_processed_protocols = plenaries_processed_jsonl_protocols_files
lock = Lock()

def init_child(lock_):
    global lock
    lock = lock_
def non_valid_id_treatment(e, file_path, data):
    parse_succeeded = False
    error = e
    while "is not a valid ID." in error.args[0]:
        id = error.args[0].split(": ")[1].split()[0].replace("\'", "")
        file_name = os.path.basename(file_path)
        print(f'in file {file_name}: invalid id: {id}')
        param = f'{id}\t'
        pattern = '\n{}.*?\n'.format(param)
        res = re.sub(pattern, '\n', data)
        data = res
        data = data.replace(f"# speaker_name = \n", f"# speaker_name = ")

        try:
            sentences = parse(data)
            parse_succeeded = True
            break
        except Exception as e:
            error = e
    if parse_succeeded:
        with open(file_path, 'w', encoding="utf-8") as file:
            file.write(data)
    else:
        sentences = parse_each_conllu_sentence_separatly(data)
        if not sentences:
            print(f'couldnt parse {file_path}. error was: {error}')
            return []
    return sentences

def parse_each_conllu_sentence_separatly(data):
    sentences = []
    separated_data = data.split("\n\n")
    for conllu_sent in separated_data:
        try:
            sent = parse(conllu_sent)
        except:
            continue
        sentences.extend(sent)
    return sentences
def parse_protocol_conllu_file(file_path):
    if os.path.exists(file_path):
        with open(file_path, encoding="utf-8") as file:
            data = file.read()
    try:
        sentences = parse(data)
    except Exception as e:
        if "parse_int_value" in e.args[0]:
            sentences = parse_each_conllu_sentence_separatly(data)
        elif "is not a valid ID." in e.args[0]:
            sentences = non_valid_id_treatment(e, file_path, data)
        elif "line must contain either tabs or two spaces" in e.args[0]:
            data = data.replace(f"# speaker_name = \n", f"# speaker_name = ")
            try:
                sentences = parse(data)
                with open(file_path, 'w', encoding="utf-8") as file:
                    file.write(data)
            except Exception as e:
                if "is not a valid ID." in e.args[0]:
                    sentences = non_valid_id_treatment(e, file_path, data)
                elif "parse_int_value" in e.args[0]:
                    sentences = parse_each_conllu_sentence_separatly(data)
                else:
                    print(f'couldnt parse {file_path}. error was: {e}')
                    return

        else:
            print(f'couldnt parse {file_path}. error was: {e}')
            return
    if sentences and len(sentences)>0:
        protocol_name = sentences[0].metadata['protocol_name']
    else:
        sentences = parse_each_conllu_sentence_separatly(data)
        if sentences and len(sentences) > 0:
            protocol_name = sentences[0].metadata['protocol_name']
        else:
            print(f"something went wrong in {file_path}")
            return
    processed_protocol_path = os.path.join(path_to_processed_protocols, f'{protocol_name}.jsonl')


    # if file_modified_in_last_x_days(processed_protocol_path, 2):
    #     return

    try:
        with open(processed_protocol_path, encoding="utf-8") as file:
            protocol_json = json.load(file)
    except Exception as e:
        print(f'couldnt open json file: {processed_protocol_path}. error: {e}')
        return
    protocol_sentences = protocol_json["protocol_sentences"]
    if len(sentences) == len(protocol_sentences):
        for sent, protocol_sent in zip(sentences, protocol_sentences):
            sentence_id = sent.metadata['sentence_id']
            if sentence_id == protocol_sent["sentence_id"]:
                protocol_sent["morphological_fields"] = list(sent)
                try:
                    json_str = json.dumps(protocol_sent["morphological_fields"], ensure_ascii=False)
                    json_obj = json.loads(json_str)
                except Exception as e:
                    print(f'{e}: sent is: {list(sent)}')
                    return
            else:
                print(f"sentences ids don't match! in {protocol_name}. sent_1:{sentence_id}, sent_2: {protocol_sent['sentence_id']}")

    else:
        for sent in sentences:
            sentence_id = sent.metadata['sentence_id']
            for protocol_sent in protocol_sentences:
                if sentence_id == protocol_sent["sentence_id"]:
                    protocol_sent["morphological_fields"] = list(sent)
                    try:
                        json_str = json.dumps(protocol_sent["morphological_fields"], ensure_ascii=False)
                        json_obj = json.loads(json_str)
                    except Exception as e:
                        print(f'{e}: sent is: {list(sent)}')
                        return
                    break
    try:
        json_str = json.dumps(protocol_json, ensure_ascii=False)
        json_obj = json.loads(json_str)
    except Exception as e:
        print(e)
    with open(processed_protocol_path, 'w', encoding='utf-8') as file:
        json.dump(protocol_json, file, ensure_ascii=False)
    # print(f'finished processing {file_path}')


def worker(file_name):
    file_path = os.path.join(path_to_conllu_files, file_name)
    parse_protocol_conllu_file(file_path)

if __name__ == '__main__':
    paths = os.listdir(path_to_conllu_files)

    if MULTIPROCESSING:
        pool = Pool()
        pool.map(worker, paths)

    else:
        for file in paths:
            file_path = os.path.join(path_to_conllu_files, file)
            parse_protocol_conllu_file(file_path)