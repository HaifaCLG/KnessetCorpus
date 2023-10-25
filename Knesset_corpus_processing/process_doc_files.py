#!/usr/bin/env python
# -*- coding: utf-8 -*-
import atexit
import sys
import traceback
import win32com.client as win32
from win32com.client import constants
from docx import Document

from lists_of_known_problematic_files import too_short_protocols, speaker_name_is_empty_in_doc_protocols
from models.protocol_models import *
from params_config import *

from multiprocessing import Pool
from multiprocessing import Lock
import logging

logging.basicConfig(filename=log_file_path, level=logging.INFO,  format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S', encoding='utf-8')
lock = Lock()

def init_child(lock_):
    global lock
    lock = lock_


def save_as_docx(path, dest_path):
    dest_path_other_option = dest_path.replace('.docx', '..docx')
    if os.path.exists(dest_path) or os.path.exists(dest_path_other_option):
        return
    dest_path = dest_path.replace('..docx', '.docx')
    with lock:
        doc = None
        word = None
        try:
            print(path, flush=True)
            word = win32.gencache.EnsureDispatch('Word.Application')
            doc = word.Documents.Open(path)
            doc.Visible = False
            doc.Activate()

            # Save and Close
            word.ActiveDocument.SaveAs(
                dest_path, FileFormat=constants.wdFormatXMLDocument
            )
            doc.Close(False)
            word.Quit()
        except:
            time.sleep(1)
            delete_win32_cache()
            raise Exception(f'couldnt save doc to docx')

        finally:
            word = None
            doc = None



def delete_win32_cache():
    MODULE_LIST = [m.__name__ for m in sys.modules.values()]
    for module in MODULE_LIST:
        if re.match(r'win32com\.gen_py\..+', module):
            del sys.modules[module]
    shutil.rmtree(
        os.path.join(os.environ.get('LOCALAPPDATA'), 'Temp', 'gen_py'))  # if this won't work try the next row instead
    # shutil.rmtree(os.path.abspath(os.path.join(win32com.__gen_path__, '..')), ignore_errors=True)


def keep_only_necessary_fields_in_protocol_object(protocols):
    protocols_dicts = []
    for protocol in protocols:
        protocol_dict = protocol.__dict__
        del protocol_dict['document']
        del protocol_dict['knesset_members_df']
        protocols_dicts.append(protocol_dict)
    return protocols_dicts



def process_protocol_files(files_path, protocol_type):
    paths = get_all_files_pathes_recuresively_from_dir(files_path)
    if ONLY_UNSUCCESFUL:
        with open('non_succesfull_protocols.txt') as file:
            unsuccesful_paths = file.readlines()
            unsuccesful_paths = list(set([x.strip() for x in unsuccesful_paths]))
        paths = unsuccesful_paths
    if not RETRY_UNSUCCESFUL:
        with open('non_succesfull_protocols.txt') as file:
            unsuccesful_paths = file.readlines()
            unsuccesful_paths = [x.strip() for x in unsuccesful_paths]
        paths = [x for x in paths if x not in unsuccesful_paths]
    paths = [x for x in paths if ('.pdf' not in x and '.PDF' not in x)]
    to_delete_list = []
    for protocol_name in too_short_protocols:
        for path in paths:
            if protocol_name in path:
                to_delete_list.append(path)
    for protocol_name in speaker_name_is_empty_in_doc_protocols:
        for path in paths:
            if protocol_name in path:
                to_delete_list.append(path)
    paths = [x for x in paths if(x not in to_delete_list)]
    print(f'number of protocols to process: {len(paths)}')

    if MULTIPROCESSING:
        logging.info('starting protocol process multiprocessing')
        print(f'starting protocol process multiprocessing', flush=True)
        with Pool(processes=NUMBER_OF_PROCESSES, initializer=init_child, initargs=(lock,)) as pool:
            results = pool.map_async(func=process_protocol_file_unpack_safe,iterable=map(lambda path: (path, protocol_type), paths))
            protocol_results = results.get()

    else:
        for file_path in paths:
            output_path = get_protocol_output_path(file_path, protocol_type)
            if IGNORE_EXISTING_PROTOCOL_OUTPUT_FILES and os.path.exists(output_path):
                continue
            protocol = process_protocol_file(file_path, protocol_type)
            if protocol:
                write_protocol_to_jsonl_file(output_path, protocol)


def write_protocol_to_jsonl_file(output_path, protocol):
    protocol_dict = protocol.__dict__
    del protocol_dict['document']
    del protocol_dict['all_knesset_members_df']
    del protocol_dict['file_name']
    json_str = json.dumps(protocol_dict, default=lambda o: o.__dict__, ensure_ascii=False)
    with open(output_path, 'w', encoding='utf-8') as file:
        print(json_str, file=file)


def get_protocol_output_path(file_path, protocol_type):
    file_name = os.path.basename(file_path)
    if protocol_type == PROTOCOL_TYPES[0]:
        path = os.path.join(committees_processed_jsonl_protocols_files, file_name + '.jsonl')
    elif protocol_type == PROTOCOL_TYPES[1]:
        path = os.path.join(plenaries_processed_jsonl_protocols_files, file_name + '.jsonl')
    else:
        raise ValueError()
    return path


def process_protocol_file_unpack_safe(args):
    try:
        return process_protocol_file_unpack(args)
    except Exception as e:
        logging.error(f"process_protocol_file_unpack({args}) failed. exception was:{repr(e)}. {''.join(traceback.TracebackException.from_exception(e).format())}\n")
        logging.info(traceback.print_exception(*sys.exc_info()))


def process_protocol_file_unpack(args):
    output_path = get_protocol_output_path(args[0], args[1])
    if IGNORE_EXISTING_PROTOCOL_OUTPUT_FILES and os.path.exists(output_path):
        return
    try:
        protocol = process_protocol_file(*args)
        if protocol:
            write_protocol_to_jsonl_file(output_path, protocol)
        else:
            raise Exception(f"empty protocol: {str(args[0])}" )
    except Exception as e:
        logging.error(f"process_protocol_file({args}) failed. exception was:{e}\n", exc_info=True)
        if not ONLY_UNSUCCESFUL:
            with open('non_succesfull_protocols.txt', 'a') as file:
                file.write(f'{str(args[0])}\n')


def process_file_unpack(args):
    process_file(*args)
def safe_process_file_unpack(args):
    try:
        return process_file_unpack(args)
    except Exception as e:
        logging.error(
            f"process_file_unpack({args}) failed. exception was:{repr(e)}. {''.join(traceback.TracebackException.from_exception(e).format())}\n")

def rename_docx_2_dot_files(dir_path):
    for file_name in os.listdir(dir_path):
        if "..docx" in file_name:
            new_file_name = file_name.split("..docx")[0]
            new_file_name = new_file_name +".docx"
            new_file_path = os.path.join(dir_path,new_file_name)
            if os.path.exists(new_file_path):
                continue
            os.rename(os.path.join(dir_path,file_name), new_file_path)
def process_protocol_file(file_path, protocol_type=None):
    file_name = os.path.basename(file_path)
    if not protocol_type:
        protocol_type = is_committee_or_plenary(file_name)
        if protocol_type == EMPTY_STRING:
            logging.error(f'Error: {file_name} is not plenary nor committee. could not process file\n')
            if PRINT_ALL:
                print('Error:' + file_name + ' is not plenary nor committee. could not process file', flush=True)
            return []
    try:
        knesset_number = extract_knesset_number_as_int(file_name)
    except Exception as e:
        logging.info("could not process file: " + file_path+'\n')
        logging.info(f'{file_name} filename does not start with a number\n')
        if PRINT_ALL:
            print("could not process file: " + file_path, flush=True)
            print('filename does not start with a number')
        raise e
    is_ocr_res = is_ocr_doc(knesset_number, file_name, protocol_type)
    document = process_file(file_path, is_ocr_res)
    if document == EMPTY_STRING:
        if not is_ocr_res:
            logging.info("could not process file: " + file_path+'\n')
        return []
    try:
        protocol = create_protocol_object(document, file_name, is_ocr_res, protocol_type)
    except Exception as e:
        logging.info("could not process file: " + file_path+'\n')
        raise e
    try:
        protocol.extract_info_from_protocol_document()
    except Exception as e:
        logging.error(f"process_protocol_file({file_path}, {protocol_type}) failed. exception was:{repr(e)}\n")
        raise e
    return protocol


def create_protocol_object(document, document_name, is_ocr_res, protocol_type):
    if is_ocr_res == EMPTY_STRING:
        raise Exception('Error: is_ocr_res is empty. how did we get here?')
    if protocol_type == 'plenary':
        protocol = Plenary_Protocol(document_name, document)
    elif protocol_type == 'committee':
        protocol = Committee_Protocol(document_name, document)
    else:
        raise Exception('Error: protocol_type is not committee nor plenary. how did we get here? protocol type:' + protocol_type)
    return protocol

def is_committee_or_plenary(file_name):
    res = EMPTY_STRING
    if 'ptm' in file_name:
        res = 'plenary'
    elif 'ptv' in file_name:
        res = 'committee'
    else:
        logging.error('Error:' + file_name + 'is not a committee nor a plenary! how did we get here?\n')
        if PRINT_ALL:
            print('Error:' + file_name + 'is not a committee nor a plenary! how did we get here?')
    return res

def is_ocr_doc(knesset_number, file_name, protocol_type=EMPTY_STRING):
    if protocol_type == EMPTY_STRING:
        protocol_type = is_committee_or_plenary(file_name)
    if 'committee' == protocol_type:
        if knesset_number <= 14:
            res = True
        else:
            res = False
    elif 'plenary' == protocol_type:
        if knesset_number <= 12:
            res = True
        else:
            res = False
    else:
        res = EMPTY_STRING
    return res


def get_destination_path(file_path):
    file_name = os.path.basename(file_path)
    docx_file_name = f"{file_name.split('.')[0]}.docx"
    dest_path = os.path.join(docx_files_path, docx_file_name)
    return dest_path


def process_file(file_path, is_ocr_file):
    if is_ocr_file:
        return EMPTY_STRING
    filename, file_extension = os.path.splitext(file_path)
    if file_extension == '.pdf':
        return EMPTY_STRING
    dest_path = get_destination_path(file_path)
    if file_extension == '.doc' or file_extension == '.DOC':
        save_as_docx(file_path, dest_path)
    elif file_extension == '.docx':
        if not os.path.exists(dest_path):
            shutil.copyfile(file_path, dest_path)
    else:
        return EMPTY_STRING
    document = read_docx_file(dest_path)

    return document


def extract_knesset_number_as_int(file_name):
    return int(file_name.split('_')[0])

def read_docx_file(file_path):
    try:
        doc = Document(file_path)
    except Exception as e:
        print(e)
        doc = EMPTY_STRING
    return doc

def exit_handler():
    word = win32.gencache.EnsureDispatch('Word.Application')
    word.Quit()


def keep_only_valid_people_in_corpus(all_processed_people_path, all_valid_people_path):
    valid_ppl = []
    with open(all_processed_people_path, encoding='utf-8') as all_ppl_file:
        ppl_jsons_strs = all_ppl_file.readlines()
        for person_str in ppl_jsons_strs:
            if person_str.strip() == EMPTY_STRING:
                continue
            try:
                person_json = json.loads(person_str)
            except Exception as e:
                print(f"in person: {person_str} error: {e}")
                continue
            if person_json['last_name'] == EMPTY_STRING:
                continue
            valid_ppl.append(person_str)
    with open(all_valid_people_path, mode='wt', encoding='utf-8') as valid_file:
        valid_file.write(''.join(valid_ppl))


def keep_only_unique_ppl_in_corpus(all_processed_people_path):
    all_unique_people = set()
    with open(all_processed_people_path, encoding="utf-8") as file:
        for line in file:
            all_unique_people.add(line)
    with open(all_processed_people_path, 'w', encoding="utf-8") as f:
        for line in list(all_unique_people):
            f.write(f"{line}")

def copy_all_double_dot_files_to_new_dir(all_paths, new_dir_path):
    paths = get_all_files_pathes_recuresively_from_dir(all_paths)
    only_double_dot_paths = [x for x in paths if "..doc" in x]
    only_double_dot_paths_without_2011 = [x for x in only_double_dot_paths if "2011" not in x]
    for path in only_double_dot_paths_without_2011:
        file_name = os.path.basename(path)
        shutil.copyfile(path,os.path.join(new_dir_path,file_name))

def print_list_of_protocol_names_to_file(path_to_all_protocols, output_name):
    paths = get_all_files_pathes_recuresively_from_dir(path_to_all_protocols)
    protocol_names = [os.path.basename(path).split(".")[0] for path in paths]
    for name in protocol_names:
        with open(output_name, "a") as file:
            file.write(f'{name}\n')

if __name__ == '__main__':
    rename_docx_2_dot_files(docx_files_path)
    atexit.register(exit_handler)
    delete_win32_cache()

    process_protocol_files(valid_committee_protocols_doc_files_path, protocol_type='committee')
    logging.info('Finished processing all committee files!\n')
    process_protocol_files(valid_plenary_protocols_doc_files_path, protocol_type='plenary')
    logging.info('Finished processing all plenary files!\n')

    concat_content_of_files_in_dir_into_output_file(processed_committee_data_path, committees_processed_data_jsonl_file_path)
    print('Finished concating all committee files!', flush=True)

    concat_content_of_files_in_dir_into_output_file(processed_plenary_data_path, plenaries_processed_data_jsonl_file_path)
    print('Finished concating all plenary files!', flush=True)

    all_processed_people_path = os.path.join(all_people_path,'all_people_in_corpus_jsons.jsonl')
    concat_content_of_files_in_dir_into_output_file(people_jsons_by_protocol_path, all_processed_people_path)
    keep_only_unique_ppl_in_corpus(all_processed_people_path)
    all_valid_people_path = os.path.join(all_people_path,'all_valid_people_in_corpus_jsons.jsonl')
    keep_only_valid_people_in_corpus(all_processed_people_path,all_valid_people_path )
