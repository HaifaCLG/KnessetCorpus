import json
import shutil
import pickle
import random
from datetime import datetime
import pandas as pd
import stat
from hebrew_nlp_infoneto import split_to_sentences_request
from difflib import SequenceMatcher
from heapq import nlargest as _nlargest
from udpipe_functions import *
from conllu import parse
from itertools import islice


def get_all_files_pathes_recuresively_from_dir(main_dir_path):
    list_of_files = []

    for root, dirs, files in os.walk(main_dir_path):
        for file in files:
            file_path = os.path.join(root, file)
            info = os.stat(file_path)
            if not info.st_file_attributes & (stat.FILE_ATTRIBUTE_SYSTEM | stat.FILE_ATTRIBUTE_HIDDEN):
                list_of_files.append(file_path)
    return list_of_files


def write_records_to_csv(records, csv_file_path, columns_names):
    df = pd.DataFrame(records, columns=columns_names)
    df.to_csv(csv_file_path, index=False)


def contains_numbers(inputString):
        return any(char.isdigit() for char in inputString)

def write_dictionary_values_to_json_lines_file(dict, output_file_path):
    with open(output_file_path, 'w', encoding='utf-8') as file:
        for row in dict.values():
            json_str = json.dumps(row.__dict__, ensure_ascii=False)
            print(json_str, file=file)
    if PRINT_ALL:
        print(f'finished writing dict to jsons file!')

def write_list_of_class_subjects_to_json_lines_file(list, output_file_path):
    with open(output_file_path, 'w', encoding='utf-8') as file:
        for row in list:
            json_str = json.dumps(row.__dict__, ensure_ascii=False)
            print(json_str, file=file)
    if PRINT_ALL:
        print(f'finished writing dict to jsons file!')

def write_list_of_dicts_to_json_lines_file(list, output_file_path):
    with open(output_file_path, 'w', encoding='utf-8') as file:
        for row in list:
            json_str = json.dumps(row, ensure_ascii=False)
            print(json_str, file=file)
    if PRINT_ALL:
        print(f'finished writing dicts to jsons file!')

def read_json_lines_file_to_df(jsonl_file_path):
    with open(jsonl_file_path, encoding='utf-8') as file:
        df = pd.read_json(file, lines=True)
    return df


def split_text_to_sentences(text):
    if not text:
        return []
    res = []
    if SPLIT_SENTENCES_TOOL == 'infoneto':
        return split_to_sentences_request(text)
    elif SPLIT_SENTENCES_TOOL== 'udpipe':
        res =  udpipe_split_sentences_rest(text)
        if not res:
            logging.error(f'udpipe split sentences didnt work. trying with infoneto\n')
            res =  split_to_sentences_request(text)
            if not res:
                logging.info(f'infoneto split sentences didnt work.\n')
    return res


def get_close_matches_indexes(word, possibilities, n=3, cutoff=0.6):
    """Use SequenceMatcher to return list of the best "good enough" matches indices.

    word is a sequence for which close matches are desired (typically a
    string).

    possibilities is a list of sequences against which to match word
    (typically a list of strings).

    Optional arg n (default 3) is the maximum number of close matches to
    return.  n must be > 0.

    Optional arg cutoff (default 0.6) is a float in [0, 1].  Possibilities
    that don't score at least that similar to word are ignored.

    The best (no more than n) matches among the possibilities are returned
    in a list, sorted by similarity score, most similar first.

    >>> get_close_matches("appel", ["ape", "apple", "peach", "puppy"])
    ['apple', 'ape']
    >>> import keyword as _keyword
    >>> get_close_matches("wheel", _keyword.kwlist)
    ['while']
    >>> get_close_matches("Apple", _keyword.kwlist)
    []
    >>> get_close_matches("accept", _keyword.kwlist)
    ['except']
    """

    if not n >  0:
        raise ValueError("n must be > 0: %r" % (n,))
    if not 0.0 <= cutoff <= 1.0:
        raise ValueError("cutoff must be in [0.0, 1.0]: %r" % (cutoff,))
    result = []
    s = SequenceMatcher()
    s.set_seq2(word)
    for idx, x in enumerate(possibilities):
        s.set_seq1(x)
        if s.real_quick_ratio() >= cutoff and \
           s.quick_ratio() >= cutoff and \
           s.ratio() >= cutoff:
            result.append((s.ratio(), idx))

    # Move the best scorers to head of list
    result = _nlargest(n, result)
    # Strip scores for the best n matches
    return [x for score, x in result]

def concat_content_of_files_in_dir_into_output_file(dir_path,output_file_path):
    filenames = os.listdir(dir_path)
    with open(output_file_path, 'w', encoding='utf-8') as outfile:
        for fname in filenames:
            file_path = os.path.join(dir_path, fname)
            with open(file_path, encoding='utf-8') as infile:
                outfile.write(infile.read())
                outfile.write('\n')

def concat_csv_files_in_dir(dir_path, output_file_path=None, sort=False, sort_column_index='0'):
    df_append = pd.DataFrame()
    csv_files = os.listdir(dir_path)
    # append all files together
    for file in csv_files:
        file_path = os.path.join(dir_path, file)
        df_temp = pd.read_csv(file_path)
        df_append = df_append.append(df_temp, ignore_index=True)
    if sort:
        df_append.sort_values(sort_column_index)
    if output_file_path:
        write_records_to_csv(df_append, output_file_path, df_append.columns)
    return df_append


def merge_two_dicts(dict_1,dict_2):
    res = {**dict_1, **dict_2}
    return res

def copy_files_from_dir_to_dir(dir_a, dir_b):
    files = os.listdir(dir_a)
    for file in files:
        path = os.path.join(dir_a,file)
        new_path = os.path.join(dir_b, file)
        shutil.copyfile(path,new_path)

def file_modified_today(path):
    # get the time of the last modification of the specified path
    timestamp = os.path.getmtime(path)
    file_time = datetime.fromtimestamp(timestamp)
    now = datetime.now()
    if file_time.date() == now.date():
        return True
    else:
        return False

def file_modified_in_last_x_days(path, x):
    modified_time = os.path.getmtime(path)
    current_time = time.time()
    difference_in_seconds = current_time - modified_time
    difference_in_days = difference_in_seconds / (60 * 60 * 24)
    return difference_in_days <= x

def copy_files_from_dir_to_dir_by_files_list(dir_a, dir_b, files):
    for file in files:
        path = os.path.join(dir_a,file)
        new_path = os.path.join(dir_b, file)
        shutil.copyfile(path,new_path)

def move_files_from_dir_to_dir_by_files_list(dir_a, dir_b, files):
    for file in files:
        path = os.path.join(dir_a,file)
        new_path = os.path.join(dir_b, file)
        shutil.move(path,new_path)
def check_which_items_in_one_list_but_not_in_other(list_a, list_b):
    new_list = [item for item in list_a if item not in list_b]
    return new_list

def copy_lines_from_jsonl_to_json_files(path_to_big_file, path_to_jsonl_files):
    with open(path_to_big_file, encoding='utf-8') as big_file:
        for line in big_file:
            row = json.loads(line)
            file_name = row['protocol_name']
            jsonl_file_name = f'{file_name}.jsonl'
            protocol_json_path = os.path.join(path_to_jsonl_files,jsonl_file_name )
            with open(protocol_json_path, 'w', encoding='utf-8') as file:
                json.dump(row, file, ensure_ascii=False)
                print(row)



def create_small_sample_of_df(df, num_of_entries, save_as_csv=False, new_df_file_name="sampled_df.csv"):
    new_df = df.sample(n=num_of_entries)
    if save_as_csv:
        new_df.to_csv(new_df_file_name)
    return new_df


def take(n, iterable):
    """Return the first n items of the iterable as a list."""
    return list(islice(iterable, n))


def save_object(obj, object_name):
    obj_name = object_name.replace("/", "_")
    path = os.path.join('pickle_objects', f'{obj_name}.pkl')
    with open(path, 'wb') as file:
        pickle.dump(obj, file, pickle.HIGHEST_PROTOCOL)


def load_object(object_name):
    obj_name = object_name.replace("/", "_")
    path = os.path.join('pickle_objects', f'{obj_name}.pkl')
    with open(path, 'rb') as file:
        model_obj = pickle.load(file)
    return model_obj

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


def merge_two_large_csv_files(first_csv_file, second_csv_file, output_file_name):
    CHUNK_SIZE = 50000
    chunk_container = pd.read_csv(first_csv_file, chunksize=CHUNK_SIZE)
    First_time = True
    for chunk in chunk_container:
        if First_time:
            chunk.to_csv(output_file_name, mode="a", index=False)
            First_time = False
        else:
            chunk.to_csv(output_file_name, mode="a", index=False, header=False)

    chunk_container = pd.read_csv(second_csv_file, chunksize=CHUNK_SIZE, skiprows=[0])
    for chunk in chunk_container:
        chunk.to_csv(output_file_name, mode="a", index=False, header=False)


def sample_n_files_from_dir(dir_path, n, folder_to_copy_files=None):
    file_names = os.listdir(dir_path)
    chosen_file_names = random.sample(file_names, n)
    if folder_to_copy_files:
        for file in chosen_file_names:
            original_path = os.path.join(dir_path, file)
            dest_path = os.path.join(folder_to_copy_files, file)
            shutil.copy(original_path, dest_path)
    return chosen_file_names
