import json
import pickle
from datetime import datetime
from params_config import *
import pandas as pd
plenary_formatted_dates_path = os.path.join(processed_knesset_data_path,"protocol_dates","formatted_dates","Formatted_dates_all_plenary_dates.csv")
committee_formateed_dates_path = os.path.join(processed_knesset_data_path,"protocol_dates","formatted_dates","Formatted_dates_all_committe_dates.csv")

def read_dates_file_and_create_dates_dict(paths):
    dates_dict = {}  # file_name:date
    for path in paths:
        with open(path, encoding='utf-8') as file:
            dates_df = pd.read_csv(file, keep_default_na=False)
        for index, row in dates_df.iterrows():
            if row.is_succeeded:
                file_name = row.original_file_name
                year = row.year
                month = row.month
                day = row.day
                hour = row.hour
                minutes = row.minutes
                is_succeeded = row.is_succeeded
                if not year or year=='NONE':
                    continue
                if not month or month=='NONE':
                    continue
                if not day or day=='NONE':
                    continue
                if not hour or hour == 'NONE':
                    hour = '00'
                if not minutes or minutes=='NONE':
                    minutes = '00'
                datetime_str = f'{year}-{month}-{day} {hour}:{minutes}'
                try:
                    datetime_object = datetime.strptime(datetime_str, DATE_FORMAT)
                except:
                    print(f'in {file_name} found date is: {datetime_str} does not match format')
                    continue
                new_datetime_str = datetime_object.strftime(DATE_FORMAT)
                dates_dict[file_name] = new_datetime_str
    return dates_dict


def update_protocols_dates(protocols_json_path , dates_dict):
    protocols_files_names = os.listdir(protocols_json_path)
    for file_name in protocols_files_names:
        file_path = os.path.join(protocols_json_path, file_name)
        try:
            with open(file_path, encoding='utf-8') as file:
                protocol_entity = json.load(file)
        except:
            print(f' couldnt update {file_path}')
            continue
        protocol_name = file_name.split('.jsonl')[0]
        date = dates_dict.get(protocol_name, None)
        if date:
            protocol_entity['protocol_date'] = date
            # print(protocol_entity['protocol_name'])
            with open(file_path,'w', encoding='utf-8') as file:
                json.dump(protocol_entity, file, ensure_ascii=False)


def create_year_dict(dates_dict):
    year_dict = {}
    for protocol_name, date in dates_dict.items():
        datetime_object = datetime.strptime(date, DATE_FORMAT)
        year = datetime_object.year
        year_dict[protocol_name] = year
    return year_dict




def manually_update_dates_of_protocols(dates_dict, protocols,dates):
    for i,protocol in enumerate(protocols):
        date = dates[i]
        current_date_format = "%d/%m/%Y"

        datetime_object = datetime.strptime(date, current_date_format)
        new_datetime_str = datetime_object.strftime(DATE_FORMAT)
        dates_dict[protocol] = new_datetime_str


if __name__ == '__main__':
    dates_dict= read_dates_file_and_create_dates_dict([committee_formateed_dates_path, plenary_formatted_dates_path])
    update_protocols_dates(plenaries_processed_jsonl_protocols_files, dates_dict)
    update_protocols_dates(committees_processed_jsonl_protocols_files, dates_dict)

    print("finished updating dates")

    year_dict = create_year_dict(dates_dict)
    with open('../pickles/year_dict.pickle', 'wb') as f:
        pickle.dump(year_dict, f)
