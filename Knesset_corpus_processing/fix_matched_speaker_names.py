import json
import uuid

from params_config import *
import pandas as pd
if __name__ == '__main__':
    names_fix_path = os.path.join(processed_knesset_data_path,"protocol_matched_names","adi_fixed","names_fix_output_only_changes_new_version_2.csv")
    with open(names_fix_path, encoding='utf-8') as file:
        df = pd.read_csv(names_fix_path)
        protocol_names = set(df["protocol_name"].values)
        already_fixed = {}
        for protocol in protocol_names:
            protocol_df = df.loc[df['protocol_name'] == protocol]
            if "ptm" in protocol:
                protocol_json_path = os.path.join(plenaries_processed_jsonl_protocols_files, f'{protocol}.jsonl')
            else:
                protocol_json_path = os.path.join(committees_processed_jsonl_protocols_files, f'{protocol}.jsonl')
            if os.path.exists(protocol_json_path):
                with open(protocol_json_path, encoding="utf-8") as file:
                    protocol_json = json.load(file)
                for idx, row in protocol_df.iterrows():
                    current_speaker_name = row["assigned_name"]
                    if already_fixed.get((current_speaker_name, protocol), False):
                        continue
                    change_match = row["change_match"]
                    if change_match == True:
                        fixed_name = row["new_assigned_name"]
                        id = row["member_id"]
                        try:
                            fixed_id = str(int(id))
                        except Exception as e:
                            print(e)
                            print(f'missing id {id} in protocol {protocol} assigned_name: {current_speaker_name}')
                            continue
                    elif change_match == False:
                        fixed_name = row["original_speaker_name"]
                        fixed_name.replace('>',"")
                        fixed_name.replace('<', "")
                        fixed_id = str(uuid.uuid4())
                    else:
                        continue
                    if current_speaker_name and fixed_name and fixed_id:
                        for sent in protocol_json["protocol_sentences"]:
                            if sent["speaker_name"] == current_speaker_name:
                                sent["speaker_name"] = fixed_name
                                sent["speaker_id"] = fixed_id
                    already_fixed[(current_speaker_name, protocol)] = True

                with open(protocol_json_path, 'w', encoding='utf-8') as file:
                    json.dump(protocol_json, file, ensure_ascii=False)
            else:
                file_name = os.path.basename(protocol_json_path)
                if 'ptv' in file_name:
                    duplicates_path = os.path.join(processed_committee_data_path, "duplicates")
                else:
                    duplicates_path = os.path.join(processed_plenary_data_path, "duplicates")

                if not file_name in os.listdir(duplicates_path):
                    print(f"json path doesnt exist: {protocol_json_path}")



