import json
import pickle
import shutil
from datetime import datetime

from params_config import *

temp_all_protocols_path = raw_knesset_data_path
all_protocols_path = all_doc_protocols_path
plenary_sessions_path = os.path.join(all_protocols_path, 'plenary_sessions')
committee_deliberations_path = os.path.join(all_protocols_path, 'committee_deliberations')
ARANGE_DIRS = False
def seperate_files_into_plenaries_and_Committees(current_protocols_path,dest_protocols_path, plenary_sessions_path, committee_deliberations_path):
    # move committee_deliberations files and plenary_sessions files outside of all_protocols_path and into right dir
    if not os.path.exists(dest_protocols_path):
        os.mkdir(dest_protocols_path)
    if not os.path.exists(plenary_sessions_path):
        os.mkdir(plenary_sessions_path)
    if not os.path.exists(committee_deliberations_path):
        os.mkdir(committee_deliberations_path)
    for file in os.listdir(current_protocols_path):
        current_file_path = os.path.join(current_protocols_path, file)
        if "ptv" in file:
            new_file_path = os.path.join(committee_deliberations_path, file)
            shutil.move(current_file_path, new_file_path)
        elif "ptm" in file:
            new_file_path = os.path.join(plenary_sessions_path, file)
            shutil.move(current_file_path, new_file_path)
        else:
            print("file is not a plenary nor a committee: " + current_file_path)


def seperate_file_to_knesset_number_dirs(protocols_path):
    for file_name in os.listdir(protocols_path):
        current_file_path = os.path.join(protocols_path, file_name)
        knesset_number = file_name.split('_')[0]
        knesset_number_dir_name = 'knesset_'+str(knesset_number)
        knesset_number_path = os.path.join(protocols_path, knesset_number_dir_name)
        if not os.path.exists(knesset_number_path):
            os.mkdir(knesset_number_path)
        new_file_path = os.path.join(knesset_number_path, file_name)
        shutil.move(current_file_path, new_file_path)


def seperate_pdf_and_word_files_in_each_dir(all_protocols_path):
    for subdir, dirs, files in os.walk(all_protocols_path):
        for dir in dirs:
            if dir == 'committee_deliberations' or dir == 'plenary_sessions' or dir =='pdf_files' or dir =='word_files':
                continue
            dir_path = os.path.join(subdir, dir)
            pdf_files_path = os.path.join(dir_path, 'pdf_files')
            if not os.path.exists(pdf_files_path):
                os.mkdir(pdf_files_path)
            word_files_path = os.path.join(dir_path, 'word_files')
            if not os.path.exists(word_files_path):
                os.mkdir(word_files_path)
            for file_name in os.listdir(dir_path):
                current_file_path = os.path.join(dir_path, file_name)
                if '.pdf' in file_name or '.PDF' in file_name:
                    new_file_path = os.path.join(pdf_files_path, file_name)
                    shutil.move(current_file_path, new_file_path)
                elif '.doc' in file_name or '.DOC' in file_name:
                    new_file_path = os.path.join(word_files_path, file_name)
                    shutil.move(current_file_path, new_file_path)
                else:
                    if file_name== 'pdf_files' or file_name=='word_files':
                        continue
                    else:
                        print("This file is not a word doc not a pdf file: " +str(current_file_path))

def get_year_of_raw_protocol_by_processed_protocol(file_name, processed_protocols_jsons):
        year = "unknown"
        processed_protocol_path = os.path.join(processed_protocols_jsons, f"{file_name}.jsonl")
        if os.path.exists(processed_protocol_path):
            with open(processed_protocol_path, encoding='utf-8') as file:
                protocol_json = json.load(file)
            protocol_date = protocol_json["protocol_date"]
            if protocol_date:
                datetime_object = datetime.strptime(protocol_date, DATE_FORMAT)
                year = datetime_object.year
        return year
def seperate_files_in_dir_to_year_dirs(dir_path, file_year_dict=None,by_year_dict_method=True, by_processed_protocols=False,processed_protocols_path=None ):
    for file_name in os.listdir(dir_path):
        current_file_path = os.path.join(dir_path, file_name)
        isFile = os.path.isfile(current_file_path)
        if isFile:
            if by_year_dict_method and file_year_dict:
                year = file_year_dict.get(file_name, 'unknown')
            elif by_processed_protocols and processed_protocols_path:
                year = get_year_of_raw_protocol_by_processed_protocol(file_name, processed_protocols_path)
            else:
                print("no method selected or required arguments were not passes")
                return
            year_path = os.path.join(dir_path, str(year))
            if not os.path.exists(year_path):
                os.mkdir(year_path)
            new_file_path = os.path.join(year_path, file_name)
            shutil.move(current_file_path, new_file_path)
        elif file_name == 'unknown':
            current_dir = current_file_path
            for file in os.listdir(current_dir):
                year = file_year_dict.get(file, 'unknown')
                if year == 'unknown':
                    continue
                else:
                    year_path = os.path.join(dir_path, str(year))
                    if not os.path.exists(year_path):
                        os.mkdir(year_path)
                    new_file_path = os.path.join(year_path, file)
                    current_file_path = os.path.join(current_dir, file)
                    shutil.move(current_file_path, new_file_path)

def seperate_file_to_year_dirs(all_protocols_path, file_year_dict):
    for subdir, dirs, files in os.walk(all_protocols_path):
        for dir in dirs:
            if dir == 'word_files' or dir == 'pdf_files':
                dir_path = os.path.join(subdir, dir)
                seperate_files_in_dir_to_year_dirs(dir_path, file_year_dict)

if __name__ == '__main__':
    if ARANGE_DIRS:
        seperate_files_into_plenaries_and_Committees(temp_all_protocols_path, all_protocols_path, plenary_sessions_path, committee_deliberations_path)
        seperate_file_to_knesset_number_dirs(committee_deliberations_path)
        seperate_file_to_knesset_number_dirs(plenary_sessions_path)
        seperate_pdf_and_word_files_in_each_dir(all_protocols_path)

    with open('../pickles/year_dict.pickle', 'rb') as f:
        file_year_dict = pickle.load(f)
    seperate_file_to_year_dirs(valid_plenary_protocols_doc_files_path, file_year_dict)
    seperate_file_to_year_dirs(valid_committee_protocols_doc_files_path, file_year_dict)
