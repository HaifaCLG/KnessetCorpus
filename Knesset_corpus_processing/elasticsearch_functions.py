import requests
import json

from requests.auth import HTTPBasicAuth

from params_config import *
from elasticsearch import Elasticsearch, helpers
from elasticsearch.helpers import scan, BulkIndexError
from elastic_user_and_server_private_info import *


def protocol_to_sentences(protocol_json):
    sentences = []
    for sent in protocol_json["protocol_sentences"]:
        if "parent_session_name" not in protocol_json:
            print(f'in protocol {protocol_json["protocol_name"]} not parent_session_name')
            parent_session_name = None
        else:
            parent_session_name = protocol_json["parent_session_name"]
        sentence = {
            "_id":sent["sentence_id"],
           "knesset_number": protocol_json["knesset_number"],
           "protocol_name": protocol_json["protocol_name"],
           "protocol_number": protocol_json["protocol_number"],
           "protocol_type": protocol_json["protocol_type"],
           "session_name": protocol_json["session_name"],
            "parent_session_name": parent_session_name,
            "@timestamp": protocol_json["protocol_date"],
            "protocol_date": protocol_json["protocol_date"],
            "is_ocr_output": protocol_json["is_ocr_output"],
            "sentence_id": sent["sentence_id"],
            "speaker_id": sent["speaker_id"],
            "speaker_name": sent["speaker_name"],
            "is_valid_speaker": sent["is_valid_speaker"],
            "is_chairman": sent["is_chairman"],
            "turn_num_in_protocol": sent["turn_num_in_protocol"],
            "sent_num_in_turn": sent["sent_num_in_turn"],
            "sentence_text": sent["sentence_text"],
            "morphological_fields": sent["morphological_fields"],
            "factuality_fields": sent["factuality_fields"],


        }

        sentences.append(sentence)

    return sentences
def insert_protocol_json_to_elastic(json_entity):
    print(json_entity['protocol_name'])
    sentences = protocol_to_sentences(json_entity)
    result = helpers.bulk(es, sentences, index="protocol_sentences")
    print(result)

def insert_protocols_to_elastic(path):
    duplicates_path = os.path.join(processed_committee_data_path,"duplicates")
    json_file_names = os.listdir(path)
    for file in json_file_names:
        dups = os.listdir(duplicates_path)
        if file in dups:
            continue
        json_file_path = os.path.join(path, file)
        try:
            with open(json_file_path, encoding='utf-8') as file:
                json_entity = json.load(file)
                insert_protocol_json_to_elastic(json_entity)
        except Exception as e:
            print(e)
            continue

def insert_full_sentences_to_elastic(path_to_jsonl_file):
    index_name = 'all_features_sentences'
    max_counter = 200000
    sentences = []
    failed_documents = []
    with open(path_to_jsonl_file, encoding="utf-8") as file:
        for line in file:
            try:
                sent_json = json.loads(line)
            except Exception as e:
                print(e)
                continue

            if sent_json['morphological_fields'] and len(sent_json['morphological_fields'])<10000:
                sentences.append(sent_json)
            else:
                if sent_json['morphological_fields']:
                    print(f"too long sentence: {sent_json['sentence_id']}")
                else:
                    # print(f"morphological_fields is None in {sent_json['sentence_id']}. field is: {sent_json['morphological_fields'] }")
                    sentences.append(sent_json)

            if len(sentences) <max_counter:
                continue
            else:
                try:
                    _, failed = helpers.bulk(es, sentences, index=index_name, max_retries=10, chunk_size=2000,
                                             stats_only=True)
                    if failed:
                        failed_documents.append(failed)
                    sentences = []
                except BulkIndexError as e:
                    errors = e.args[1][0]['index']['error']['reason']
                    id = e.args[1][0]['index']['_id']
                    failed_documents.extend((id,errors))
                    for sent in sentences:
                        if sent['sentence_id'] == id:
                            sentences.remove(sent)
                    print(f'{id}: {errors}')
                except Exception as e:
                    print(f'Unexpected error: {e}')
                    sentences = []

        try:
            _, failed = helpers.bulk(es, sentences, index=index_name, max_retries=10, chunk_size=2000,
                                     stats_only=True)
            if failed:
                print(f'document failed: {failed}')
        except BulkIndexError as e:
            errors = e.args[1][0]['index']['error']['reason']
            id = e.args[1][0]['index']['_id']
            print(f'{id}: {errors}')
            failed_documents.extend((id, errors))
        except Exception as e:
            print(f'Unexpected error during final insert: {e}')

        print(f'Total documents failed: {len(failed_documents)}')
        for item in failed_documents:
            print(item)




def insert_person_json_to_elastic(json_entity):
    url = f"http://{elastic_ip}/people/_doc/{json_entity['person_id']}"

    payload = json.dumps(json_entity)
    headers = {'Content-Type': 'application/json'}
    response = requests.request("POST", url, headers=headers, data=payload)
    print(response.text)

def insert_faction_json_to_elastic(json_entity):
    url = f"http://{elastic_ip}/factions/_doc/{json_entity['faction_id']}"

    payload = json.dumps(json_entity)
    headers = {'Content-Type': 'application/json'}
    response = requests.request("POST", url, headers=headers, data=payload, verify=False)
    print(response.text)

def insert_factions_to_elastic(factions_jsonl_file_path):
    with open(factions_jsonl_file_path, encoding='utf-8') as file:
        json_strs = file.readlines()
        for json_str in json_strs:
            json_entity = json.loads(json_str)
            insert_faction_json_to_elastic(json_entity)


def insert_people_to_elastic(people_jsonl_file_path):
    with open(people_jsonl_file_path, encoding='utf-8') as file:
        json_strs = file.readlines()
        for json_str in json_strs:
            json_entity = json.loads(json_str)
            insert_person_json_to_elastic(json_entity)

def insert_knesset_json_to_elastic(json_entity):
    url = f"http://{elastic_ip}/knessets/_doc/{json_entity['knesset_number']}"
    payload = json.dumps(json_entity)
    headers = {'Content-Type': 'application/json'}
    response = requests.request("POST", url, headers=headers, data=payload)
    print(response.text)

def insert_knessets_to_elastic(knesset_jsonl_file_path):
    with open(knesset_jsonl_file_path, encoding='utf-8') as file:
        json_strs = file.readlines()
        for json_str in json_strs:
            json_entity = json.loads(json_str)
            insert_knesset_json_to_elastic(json_entity)


def setup_index(url, json_schema):
    headers = {'Content-Type': 'application/json'}
    auth = HTTPBasicAuth(es_username, es_password)
    res = requests.request("DELETE", url, headers=headers, auth=auth)
    print(res)

    payload = json.dumps(json_schema)
    response = requests.request("PUT", url, headers=headers, data=payload, auth=auth)
    print(response.text)


def setup_indices(index_name, schema_file_path):
    with open(schema_file_path, encoding='utf-8') as file:
        schema = json.load(file)
    setup_index(  f"http://{elastic_ip}/{index_name}", schema)




if __name__ == '__main__':
    setup_indices(index_name = 'all_features_sentences', schema_file_path ="../es_index_mappings/joined_sentences.json")
    insert_people_to_elastic(all_knesset_members_path)
    insert_knessets_to_elastic(knessets_jsonl_file_path)
    insert_factions_to_elastic(factions_jsonl_file_path)
    insert_full_sentences_to_elastic(all_plenary_full_sentences_jsonl_file)
    insert_full_sentences_to_elastic(all_committee_full_sentences_jsonl_file)
