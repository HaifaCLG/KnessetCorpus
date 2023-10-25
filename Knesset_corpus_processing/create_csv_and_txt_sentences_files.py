import json
import os.path

import pandas as pd

from aux_functions import merge_two_large_csv_files
from elasticsearch_functions import protocol_to_sentences
from params_config import *

def read_data_and_save_as_sentences_csv():
    plenary_jsonl_path =plenaries_processed_data_jsonl_file_path
    committee_jsonl_path = committees_processed_data_jsonl_file_path
    knesset_members_df = pd.read_json(all_knesset_members_path, encoding="utf-8", lines=True)
    plenary_df_flat = convert_protocols_jsonl_to_sentences_csv(plenary_jsonl_path, output_name=plenary_processed_protocol_sentences_csv_file)
    committee_df_flat = convert_protocols_jsonl_to_sentences_csv(committee_jsonl_path, output_name=committee_processed_protocol_sentences_csv_file)

def convert_protocols_jsonl_to_sentences_csv(protocols_jsonl_path, output_name="processed_protocols.csv"):
    First_df = True
    with open(protocols_jsonl_path, encoding="utf-8") as file:
        for line in file:
            if line.strip() == "":
                continue
            try:
                protocol_json = json.loads(line)
            except Exception as e:
                print(f'in {line}, error: {e}')
                continue
            protocol_sentences = protocol_to_sentences(protocol_json)

            protocol_df = pd.DataFrame.from_records(protocol_sentences)
            if First_df:
                protocol_df.to_csv(output_name, index=False)
                First_df = False
            else:
                protocol_df.to_csv(output_name, mode='a', header=False, index=False)

def create_knesset_members_sentences_df(protocols_jsonl_paths,knesset_members_df ,save_as_csv=False):
    knesset_members_sentences = []
    knesset_members_df = knesset_members_df.astype({'person_id': 'string'})
    knesset_members_ids = list(knesset_members_df["person_id"])
    for path in protocols_jsonl_paths:
        with open(path, encoding='utf-8') as file:
            for line in file:
                if line.strip() == "":
                    continue
                try:
                    protocol_json_struct = json.loads(line)
                except Exception as e:
                    print(f'error in line: {line}: error:{e}\n')
                    continue
                protocol_sentences = protocol_to_sentences(protocol_json_struct)
                for sent in protocol_sentences:
                    if sent["speaker_id"] in knesset_members_ids:
                        knesset_members_sentences.append(sent)
    all_knesset_members_sentences_df = pd.DataFrame(knesset_members_sentences)

    if save_as_csv:
        all_knesset_members_sentences_df.to_csv(os.path.join(knesset_csv_files_path,"all_knesset_members_sentences.csv"))
    return all_knesset_members_sentences_df

def create_knesset_members_df_and_csv_files():
    knesset_members_df = pd.read_json(all_knesset_members_path,
                                      encoding="utf-8", lines=True)
    knesset_members_df = knesset_members_df.astype({'person_id': 'string'})
    knesset_members_sentences_df = create_knesset_members_sentences_df([plenary_jsonl_path, committee_jsonl_path],
                                                                       knesset_members_df, save_as_csv=False)
    knesset_members_sentences_df = knesset_members_sentences_df.rename(columns={"speaker_id": "person_id"})
    knesset_members_sentences_df = knesset_members_sentences_df.astype({'person_id': 'string'})
    knesset_members_sentences_and_person_info_df = knesset_members_sentences_df.merge(knesset_members_df,
                                                                                      on="person_id", how="left")
    knesset_members_sentences_and_person_info_df.to_csv("knesset_members_sentences_and_person_info.csv")
    knesset_members_sentences_and_person_info_df = pd.read_csv("knesset_members_sentences_and_person_info.csv")
    knesset_members_sentences_and_person_info_df.drop(columns=['Unnamed: 0', '_id', '@timestamp'])
    return knesset_members_sentences_and_person_info_df


def write_only_sentences_text_to_file(all_protocols_chunk_container, output_file_name="all_sentences_text.txt"):
    for chunk in all_protocols_chunk_container:
        texts = list(chunk['sentence_text'].values)
        with open(output_file_name, 'a', encoding='utf-8') as file:
            for text in texts:
                file.write(f'{text}\n')

if __name__ == '__main__':
    plenary_jsonl_path = plenaries_processed_data_jsonl_file_path
    committee_jsonl_path = committees_processed_data_jsonl_file_path
    read_data_and_save_as_sentences_csv()
    merge_two_large_csv_files(plenary_processed_protocol_sentences_csv_file,committee_processed_protocol_sentences_csv_file, all_protocols_sentences_csv_file)

    knesset_members_sentences_and_person_info_df = create_knesset_members_df_and_csv_files()

    all_protocols_chunk_container = pd.read_csv(all_protocols_sentences_csv_file, chunksize=10000)
    write_only_sentences_text_to_file(all_protocols_chunk_container,
                                      output_file_name=all_sentences_text_file)
    all_committee_sentences_chunk_container = pd.read_csv(committee_processed_protocol_sentences_csv_file,
        chunksize=10000)
    write_only_sentences_text_to_file(all_committee_sentences_chunk_container,
                                      output_file_name=committee_sentences_text_file)
    all_plenaries_sentences_chunk_container = pd.read_csv(
        plenary_processed_protocol_sentences_csv_file,
        chunksize=10000)
    write_only_sentences_text_to_file(all_plenaries_sentences_chunk_container,
                                      output_file_name=plenary_sentences_text_file)


