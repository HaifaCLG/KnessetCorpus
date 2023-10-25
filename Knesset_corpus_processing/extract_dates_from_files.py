from process_doc_files import process_protocol_files, process_file, create_protocol_object
from aux_functions import *
import logging

logging.basicConfig(filename=log_file_path, level=logging.INFO,  format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S', encoding='utf-8')

def extract_name_and_date_from_protocols_to_df(files_path, protocol_type, output_file_path):
    wanted_knessets_files = ['1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20', '21','22','23', '24']
    unwanted_knesset_files = []
    paths = get_all_files_pathes_recuresively_from_dir(files_path)

    records = []
    for file_path in paths:
        file_name = os.path.basename(file_path)
        knesset_number = file_name.split('_')[0]
        if knesset_number not in wanted_knessets_files:
            continue
        document = process_file(file_path, False)
        if document == EMPTY_STRING:
            logging.error("could not process file: " + file_path+'\n')
            print("could not process file: " + file_path, flush=True)
            continue
        try:
            protocol = create_protocol_object(document, file_path, False, protocol_type)
        except:
            logging.error("could not process file: " + file_path+'\n')
            print("could not process file: " + file_path, flush=True)
            continue
        date = protocol.extract_protocol_date()
        if date:
            record = (file_name, date)
            records.append(record)
        else:
            logging.info(f'date was not found in file {file_name}\n')
            print(f'date was not found in file {file_name}')
        print(f'finished dealing with file: {file_name}')
    file_name_and_date_columns = ["original_file_name", "protocol_date"]
    records_df = pd.DataFrame(records, columns=file_name_and_date_columns)
    write_records_to_csv(records_df, output_file_path, file_name_and_date_columns)



if __name__ == '__main__':
    protocol_dates_path = os.path.join(processed_knesset_data_path,"protocol_dates")
    shutil.rmtree(os.path.join(os.environ.get('LOCALAPPDATA'), 'Temp', 'gen_py'))
    files_path = valid_committee_protocols_doc_files_path
    protocol_type = 'committee'
    output_file_path = os.path.join(protocol_dates_path,"all_committee_dates.csv")
    extract_name_and_date_from_protocols_to_df(files_path, protocol_type, output_file_path)


    files_path = valid_plenary_protocols_doc_files_path
    protocol_type = 'plenary'
    output_file_path = os.path.join(protocol_dates_path,'all_plenary_dates.csv')
    extract_name_and_date_from_protocols_to_df(files_path, protocol_type, output_file_path)
    # concat_csv_files_in_dir(os.path.join(protocol_dates_path,'plenary'), output_file_path)


