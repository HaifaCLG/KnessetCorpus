import json
import logging
import shutil
import pandas as pd

from aux_functions import write_records_to_csv, get_all_files_pathes_recuresively_from_dir, \
    check_which_items_in_one_list_but_not_in_other
from lists_of_known_problematic_files import too_short_protocols, speaker_name_is_empty_in_doc_protocols
from params_config import *
from datetime import datetime


def check_duplicity_in_plenary_protocols(path):
    protocol_numbers = []
    protocol_names = []
    # protocol_dates = []
    dups_to_delete = []
    duplicates_counter = 0
    file_names = os.listdir(path)
    number_of_files = len(file_names)
    for file_name in file_names:
        file_path = os.path.join(path, file_name)
        with open(file_path, encoding='utf-8') as file:
            protocol_entity = json.load(file)
        protocol_number = protocol_entity['protocol_number']
        protocol_name = protocol_entity['protocol_name']
        protocol_date = protocol_entity['protocol_date']
        if protocol_number in protocol_numbers:
            index = protocol_numbers.index(protocol_number)
            # if protocol_date == protocol_dates[index]:
            print(
                f' protocol {protocol_names[index]} and protocol {protocol_name} have same protocl number: {protocol_number}')
            duplicates_counter += 1
            protocol_name_num_in_list = get_number_from_protocol_name(protocol_names[index])
            new_protocol_name_num = get_number_from_protocol_name(protocol_name)
            if new_protocol_name_num > protocol_name_num_in_list:
                dups_to_delete.append(protocol_names[index])
                del protocol_numbers[index]
                del protocol_names[index]
                # del protocol_dates[index]
                protocol_numbers.append(protocol_number)
                protocol_names.append(protocol_name)
                # protocol_dates.append(protocol_date)
            else:
                dups_to_delete.append(protocol_name)
        else:
            protocol_numbers.append(protocol_number)
            protocol_names.append(protocol_name)
            # protocol_dates.append(protocol_date)
    print(f'number of duplicates: {duplicates_counter}')
    print(f'total number of files: {number_of_files}')
    return dups_to_delete

def check_validitiy_of_plenary_protocols(path):
    verified_protocols_names = ['13_ptm_532015.docx', '13_ptm_532030.docx', '13_ptm_532073.docx', '13_ptm_532112.docx',
                                '13_ptm_532259.docx', '13_ptm_532291.docx', '13_ptm_532435.docx', '13_ptm_532445.docx',
                                '13_ptm_532446.docx', '14_ptm_532473.docx', '15_ptm_532879.docx']
    good_counter = 0
    bad_counter = 0
    file_names = os.listdir(path)
    number_of_files = len(file_names)
    for file_name in file_names:
        file_path = os.path.join(path, file_name)
        with open(file_path, encoding='utf-8') as file:
            protocol_entity = json.load(file)
        sentences = protocol_entity['protocol_sentences']
        if not sentences:
            logging.info(f"protocol is empty: {file_names}")
            bad_counter += 1
            continue
        last_sent_text = sentences[-1]['sentence_text']
        if protocol_entity[
            'protocol_name'] in verified_protocols_names or 'נעולה' in last_sent_text or 'ננעלה' in last_sent_text or 'נועל' in last_sent_text or 'נועלת' in last_sent_text or 'סוגרת' in last_sent_text or 'סוגר' in last_sent_text or 'סגורה' in last_sent_text or 'הסתיימה' in last_sent_text or 'תמה' in last_sent_text or 'תודה' in last_sent_text or 'לילה טוב' in last_sent_text or 'הישיבה הבאה' in last_sent_text or 'שבת שלום' in last_sent_text or 'יום טוב' in last_sent_text or 'מודה' in last_sent_text or 'ערב טוב' in last_sent_text or 'שלום' in last_sent_text or 'חג' in last_sent_text or 'שנה טובה' in last_sent_text or 'שבוע טוב' in last_sent_text or 'נעילת' in last_sent_text or 'נעולים' in last_sent_text or 'שמח' in last_sent_text or 'שבת' in last_sent_text or 'סעו בזהירות' in last_sent_text or 'הבאה' in last_sent_text or 'להתראות' in last_sent_text or 'תם' in last_sent_text or 'סיימנו' in last_sent_text or 'נעול' in last_sent_text or 'ננעלת' in last_sent_text or 'מחיאות כפיים' in last_sent_text or 'ברוכים' in last_sent_text or 'כל טוב' in last_sent_text or 'בוקר טוב' in last_sent_text or 'חופשה נעימה' in last_sent_text or 'צלחה' in last_sent_text or 'נסתיימה' in last_sent_text or 'נחדש' in last_sent_text or 'יתחדש' in last_sent_text or 'תתכנס' in last_sent_text or 'להתכנס' in last_sent_text or 'שעה' in last_sent_text or 'השם' in last_sent_text or 'פגרה נעימה' in last_sent_text:
            good_counter += 1
            continue
        else:
            logging.info(f"protocol didn't end with meeting ending: {file_name}")
            print(f"protocol didn't end with meeting ending: {file_name}, last sentence was: {last_sent_text}")
            bad_counter += 1
            continue

    print(f' number_of_files: {number_of_files}')
    print((f'number of valid files : {good_counter}'))
    print((f'number of non valid files : {bad_counter}'))


def change_sentences_ids_to_be_as_original_samples(processed_data_dir_path, sampled_sentences_path,
                                                   protocol_type='plenary'):
    sampled_file_names = os.listdir(sampled_sentences_path)
    for file_name in sampled_file_names:
        if protocol_type == "plenary" and 'ptv' in file_name:
            continue
        elif protocol_type == 'committee' and 'ptm' in file_name:
            continue

        sampled_file_path = os.path.join(sampled_sentences_path, file_name)
        if os.path.isdir(sampled_file_path):
            continue
        protocol_file_name = file_name.rsplit('_', 1)[0]
        protocol_file_path = os.path.join(processed_data_dir_path, f'{protocol_file_name}.jsonl')

        try:
            with open(sampled_file_path, encoding='utf-8') as file:
                sampled_sentence_json = json.load(file)
        except Exception as e:
            print(f'couldnt open file: {sampled_file_path} ')
            print(f' exception was :{e}')
            continue
        sampled_sent_id = sampled_sentence_json['sentence_id']
        try:
            with open(protocol_file_path, encoding='utf-8') as file:
                protocol_json = json.load(file)
        except Exception as e:
            print(e)
            continue
        protocol_sentences = protocol_json['protocol_sentences']
        new_protocol_sentences = []
        for sentence in protocol_sentences:
            new_sent = sentence
            if sentence['sentence_text'] == sampled_sentence_json['sentence_text']:
                new_sent['sentence_id'] = sampled_sent_id
            new_protocol_sentences.append(new_sent)
        protocol_json['protocol_sentences'] = new_protocol_sentences
        with open(protocol_file_path, 'w', encoding='utf-8') as file:
            json.dump(protocol_json, file, ensure_ascii=False)


def get_number_from_protocol_name(protocol_name):
    name = protocol_name.split('.')[0]
    num = name.split('_')[-1]
    return int(num)


def check_duplicity_in_committee_protocols(path):
    protocols_data = []
    protocol_names = []
    dups_to_delete = []
    duplicates_counter = 0
    file_names = os.listdir(path)
    number_of_files = len(file_names)
    for file_name in file_names:
        file_path = os.path.join(path, file_name)
        try:
            with open(file_path, encoding='utf-8') as file:
                protocol_entity = json.load(file)
        except Exception as e:
            print(f'couldnt load json {file_name}. error was: {e}')
            continue
        protocol_number = protocol_entity['protocol_number']
        protocol_name = protocol_entity['protocol_name']
        session_name = protocol_entity['session_name']
        knesset_number = protocol_entity['knesset_number']
        num_of_sentences = len(protocol_entity['protocol_sentences'])
        protocol_sents = protocol_entity['protocol_sentences']
        protocol_date = protocol_entity["protocol_date"]
        if protocol_sents:
            first_speaker_name = protocol_entity['protocol_sentences'][0]['speaker_name']
        else:
            first_speaker_name = ''
        protocol_data = (protocol_number, protocol_date, session_name, knesset_number, num_of_sentences, first_speaker_name)
        if protocol_data in protocols_data:
            index = protocols_data.index(protocol_data)
            print(
                f' protocol {protocol_names[index]} and protocol {protocol_name} have same protocol data: {protocol_data}')
            duplicates_counter += 1
            protocol_name_num_in_list = get_number_from_protocol_name(protocol_names[index])
            new_protocol_name_num = get_number_from_protocol_name(protocol_name)
            if new_protocol_name_num > protocol_name_num_in_list:
                dups_to_delete.append(protocol_names[index])
                del protocols_data[index]
                del protocol_names[index]
                protocols_data.append(protocol_data)
                protocol_names.append(protocol_name)
            else:
                dups_to_delete.append(protocol_name)

        else:
            protocols_data.append(protocol_data)
            protocol_names.append(protocol_name)
    print(f'number of duplicates: {duplicates_counter}')
    print(f'total number of files: {number_of_files}')
    return dups_to_delete


def move_dups_to_different_folder(dups_to_delete, path_to_raw_data, path_to_processed_data, raw_dup_dir,processed_dup_dir):
    for protocol_name in dups_to_delete:
        raw_protocol_name = protocol_name
        knesset_number = protocol_name.split('_')[0]
        raw_protocol_path_word_files_dir = os.path.join(path_to_raw_data, f'knesset_{knesset_number}', 'word_files' )
        raw_protocol_path_year_dir_options = os.listdir(raw_protocol_path_word_files_dir)
        for year_option in raw_protocol_path_year_dir_options:
            raw_protocol_path = os.path.join(path_to_raw_data, f'knesset_{knesset_number}', 'word_files',year_option,
                                             raw_protocol_name)
            if os.path.exists(raw_protocol_path):
                break

        raw_dest_path = os.path.join(raw_dup_dir, raw_protocol_name)
        shutil.move(raw_protocol_path, raw_dest_path)

        processed_protocol_name = f'{protocol_name}.jsonl'
        processed_protocol_path = os.path.join(path_to_processed_data, processed_protocol_name)
        processed_dest_path = os.path.join(processed_dup_dir, processed_protocol_name)
        shutil.move(processed_protocol_path, processed_dest_path)

def create_list_of_plenary_files(processed_protocols_path, output_file_path):
    protocols_list = []
    for file in os.listdir(processed_protocols_path):
        file_path = os.path.join(processed_protocols_path, file)
        with open(file_path, encoding='utf-8') as file:
            protocol_json = json.load(file)
        protocol_name = protocol_json['protocol_name']
        knesset_num = protocol_json['knesset_number']
        protocol_date = protocol_json['protocol_date']
        protocol_number = protocol_json['protocol_number']
        protocol_record = (protocol_name, knesset_num, protocol_date, protocol_number)
        protocols_list.append(protocol_record)


    protocols_list = sorted(protocols_list, key=lambda x: x[2])
    protocols_columns = ["protocol_name", "knesset_number","protocol_date", "protocol_number_or_title"]
    protocols_records_df = pd.DataFrame(protocols_list, columns=protocols_columns)
    write_records_to_csv(protocols_records_df, output_file_path, protocols_columns)


def check_which_protocols_were_not_processed(protocols_path, processed_data_dir_path):
    all_protocol_names = []
    protocols_paths = get_all_files_pathes_recuresively_from_dir(protocols_path)
    for path in protocols_paths:
        protocol_name = os.path.basename(path)
        if 'pdf' in protocol_name or 'PDF' in protocol_name:
            continue
        all_protocol_names.append(protocol_name)
    processed_protocol_names = os.listdir(processed_data_dir_path)
    processed_protocols = [x.split('.jsonl')[0] for x in processed_protocol_names]
    not_processed_protocols = [x for x in all_protocol_names if x not in processed_protocols]
    not_processed_protocols_with_reason = []
    too_short_protocol_names = []
    for short_protocol_name in too_short_protocols:
        for protocol_name in not_processed_protocols:
            if short_protocol_name in protocol_name:
                too_short_protocol_names.append(protocol_name)
    empty_speaker_name_protocols = []
    for empty_speaker_protocol in speaker_name_is_empty_in_doc_protocols:
        for protocol_name in not_processed_protocols:
            if empty_speaker_protocol in protocol_name:
                empty_speaker_name_protocols.append(protocol_name)
    for name in not_processed_protocols:
        if name in too_short_protocol_names:
            new_name =  f'{name}- protocol is too short'
        elif name in empty_speaker_name_protocols:
            new_name = f'{name}- protocol has empty speaker names'
        else:
            new_name = name
        not_processed_protocols_with_reason.append(new_name)
    print('\n'.join(not_processed_protocols_with_reason))



def create_list_of_duplicates_paths(processed_dup_path):
    mt_raw_dup_paths_params = []
    file_names = os.listdir(processed_dup_path)
    for name in file_names:
        protocol_path = os.path.join(processed_dup_path, name)
        with open(protocol_path, encoding='utf-8') as file:
            protocol = json.load(file)
        protocol_name = protocol['protocol_name']
        knesset_num = protocol['knesset_number']
        date = protocol['protocol_date']
        if date:
            datetime_object = datetime.strptime(date, DATE_FORMAT)
            year = datetime_object.year
        else:
            year = "unknown"
        protocol_type = protocol["protocol_type"]
        if protocol_type == 'plenary':
            type_folder = "plenary_sessions"
        else:
            type_folder = "committee_deliberations"
        mt_raw_dup_paths_params.append({'type_folder':type_folder, 'knesset_num':f'knesset_{knesset_num}','year': str(year), 'protocol_name':protocol_name})
    return mt_raw_dup_paths_params


def move_dups_for_mt(raw_dups_paths, dest_dups_path):
    counter = 0
    for path_params in raw_dups_paths:
        path = os.path.join("/data2/Knesset-new/protocols/",path_params['type_folder'],path_params['knesset_num'], "word_files", path_params['year'], path_params['protocol_name'])
        try:
            shutil.move(path, os.path.join(dest_dups_path, path_params['protocol_name']))
        except Exception as e:
            print(e)
            continue
        counter +=1
    print(f"moved {counter} duplicated files out of {len(raw_dups_paths)}")

def get_processed_protocols_names_by_raw_protocols_dir(dir_of_raw_protocols, dir_of_processed_protocols):
    raw_protocols_names = os.listdir(dir_of_raw_protocols)
    processed_protocols_names = os.listdir(dir_of_processed_protocols)
    processed_names_of_raw_names = [f'{name}.jsonl' for name in raw_protocols_names]
    only_wanted_processed_names = [name for name in processed_protocols_names if name in processed_names_of_raw_names]
    return only_wanted_processed_names
def check_non_processed_protocols_in_dir(dir_of_raw_protocols, dir_of_processed_protocols):
    raw_protocols_names = os.listdir(dir_of_raw_protocols)
    processed_protocols_names = os.listdir(dir_of_processed_protocols)
    original_processed_protocols_names = [name.split(".jsonl")[0] for name in processed_protocols_names]
    non_processed_protocols = check_which_items_in_one_list_but_not_in_other(raw_protocols_names, original_processed_protocols_names)
    for name in non_processed_protocols:
        print(name)
    return non_processed_protocols

def find_missing_raw_protocol_files(path_to_raw_files, list_of_files_path, create_new_list_of_files_file = False):
    paths = get_all_files_pathes_recuresively_from_dir(path_to_raw_files)
    our_protocol_files = [file.split("\\")[-1] for file in paths]
    list_of_files_df = pd.read_csv(list_of_files_path)
    all_files_names = list_of_files_df["protocolFile"]
    file_names = [name.split("\\")[-1] for name in all_files_names]
    missing_files = [file for file in file_names if file not in our_protocol_files]
    if create_new_list_of_files_file:
        new_rows = []
        new_columns = list(list_of_files_df.columns)
        new_columns.append("missing_file")
        for index, row in list_of_files_df.iterrows():
            new_row = row.copy()
            if row["protocolFile"].split("\\")[-1] in missing_files:
                new_row["missing_file"] = "V"
            else:
                new_row["missing_file"] = ""
            new_rows.append(new_row)
        new_df = pd.DataFrame(new_rows, columns=new_columns)
        new_name = f"with_missing_files_column_{os.path.basename(list_of_files_path)}"
        output_path = os.path.join(os.path.dirname(list_of_files_path), new_name)
        new_df.to_csv(output_path, index=False)
    for file in missing_files:
        print(file)

def update_protocols_with_confirmed_meta_data(meta_data_list_file, path_to_processed_protocols, protocol_type="committee"):
    UPDATE_SESSION_NAME = True
    meta_data_df = pd.read_csv(meta_data_list_file)
    protocol_changed = False
    for _, row in meta_data_df.iterrows():
        file_name = row["protocolFile"].split("\\")[-1]
        processed_protocol_name = f'{file_name}.jsonl'
        processed_protocol_path = os.path.join(path_to_processed_protocols, processed_protocol_name)
        if os.path.exists(processed_protocol_path):
            try:
                with open(processed_protocol_path, encoding='utf-8') as file:
                    protocol_entity = json.load(file)
            except Exception as e:
                print(f"couldnt load json {processed_protocol_name}. error: {e}")
                continue
        else:
            continue
        file_date = row["sessionDate"]
        date_format = '%d/%m/%Y'
        datetime_object = datetime.strptime(file_date, date_format)
        year = datetime_object.year
        month = datetime_object.month
        day = datetime_object.day
        current_protocol_date = protocol_entity["protocol_date"]
        if current_protocol_date:
            current_datetime_object = datetime.strptime(current_protocol_date, DATE_FORMAT)
            current_year = current_datetime_object.year
            current_month = current_datetime_object.month
            current_day = current_datetime_object.day
        else:
            current_year = ""
            current_month = ""
            current_day = ""
        if current_year != year or current_month != month or current_day != day:
            new_date = datetime_object.strftime(DATE_FORMAT)
            protocol_entity["protocol_date"] = new_date
            protocol_changed = True
        session_number = row["sessionNumber"]
        if str(session_number) != 'nan':
            if protocol_entity["protocol_number"] != str(int(session_number)):
                protocol_entity["protocol_number"] = str(int(session_number))
                protocol_changed = True
        if UPDATE_SESSION_NAME:
            if protocol_type == "committee":
                session_name = row["committeeName"].strip()
                parent_session_name = row["parentCommittee"]
                if str(parent_session_name) == 'nan':
                    parent_session_name = None
                # if protocol_entity["session_name"] != session_name.strip():
                #     protocol_entity["session_name"] = session_name
            elif protocol_type == "plenary":
                session_name = "ישיבת מליאה"
                parent_session_name = None
            else:
                print(f"error: wrong protocol type")
                return
            protocol_entity["session_name"] = session_name
            protocol_entity = add_parent_session_key_in_right_order(parent_session_name, protocol_entity)
            protocol_changed = True
        if protocol_changed:
            try:
                with open(processed_protocol_path, 'w', encoding='utf-8') as file:
                    json.dump(protocol_entity, file, ensure_ascii=False)
            except Exception as e:
                print(f"couldnt dump json {processed_protocol_name} error: {e}")
                continue
        protocol_changed = False

def add_missing_parent_session_field(processed_protocols_path):
    for file in os.listdir(processed_protocols_path):
        protocol_changed = False

        processed_protocol_path = os.path.join(processed_protocols_path, file)
        try:
            with open(processed_protocol_path, encoding='utf-8') as file:
                protocol_entity = json.load(file)
        except Exception as e:
            print(f"couldnt load json {file}. error: {e}")
            continue

        if "parent_session_name" not in protocol_entity:
            protocol_entity = add_parent_session_key_in_right_order(None, protocol_entity)
            protocol_changed = True
        if protocol_changed:
            try:
                with open(processed_protocol_path, 'w', encoding='utf-8') as file:
                    json.dump(protocol_entity, file, ensure_ascii=False)
            except Exception as e:
                print(f"couldnt dump json {file} error: {e}")
                continue
def add_parent_session_key_in_right_order(parent_session_name, protocol_entity):
    before_key = "session_name"
    new_item = {"parent_session_name": parent_session_name}
    res = dict()
    for key in protocol_entity:
        if key == "parent_session_name":
            continue
        res[key] = protocol_entity[key]
        # modify after adding K key
        if key == before_key:
            res.update(new_item)
    protocol_entity = res
    return protocol_entity


def check_if_can_load_jsons_in_dir(path_to_jsons):
    names = os.listdir(path_to_jsons)
    num_of_files = len(names)
    num_of_corrputed = 0
    for name in names:
        path = os.path.join(path_to_jsons, name)
        try:
            with open(path, encoding='utf-8') as file:
                protocol_json = json.load(file)
        except Exception as e:
            print(f"couldnt load json. file is: {name}. error was: {e}.")
            num_of_corrputed += 1
    print(f'{num_of_corrputed} corrputed files out of total of {num_of_files}')

if __name__ == '__main__':
    check_if_can_load_jsons_in_dir(committees_processed_jsonl_protocols_files)
    check_if_can_load_jsons_in_dir(plenaries_processed_jsonl_protocols_files)

    find_missing_raw_protocol_files(os.path.join(meta_data_files_path,"all_committees_protocols_knessets_13-24.csv"), create_new_list_of_files_file=False)
    update_protocols_with_confirmed_meta_data(os.path.join(meta_data_files_path,"all_committees_protocols_knessets_13-24.csv"), committees_processed_jsonl_protocols_files, protocol_type="committee")
    update_protocols_with_confirmed_meta_data(os.path.join(meta_data_files_path,"all_plenary_protocols_knessets_13-24.csv"), plenaries_processed_jsonl_protocols_files, protocol_type="plenary")
    add_missing_parent_session_field(committees_processed_jsonl_protocols_files)
    add_missing_parent_session_field(plenaries_processed_jsonl_protocols_files)
    check_validitiy_of_plenary_protocols(processed_plenary_data_path)

    dups_to_delete = check_duplicity_in_committee_protocols(committees_processed_jsonl_protocols_files)
    print(f'in committees num of dups to delete is {len(dups_to_delete)}')

    dups_to_delete = check_duplicity_in_plenary_protocols(plenaries_processed_jsonl_protocols_files)
    print(f'in plenaries num of dups to delete is { len(dups_to_delete)}')
