import datetime
import json
from elasticsearch_functions import protocol_to_sentences
from params_config import *
from models.person_model import Person
from datetime import datetime

def update_current_faction(sent, person):
    known_and_checked_missing_factions= ["דורון שמואלי", "נחמן שי", "בני שליטא", "אוסאמה סעדי", "מיקי לוי","דוד מגן","יוסי ביילין", "גדעון בן-ישראל"]
    try:
        protocol_date = datetime.strptime(sent["protocol_date"], DATE_FORMAT)
    except:
        sent["faction_id"] = None
        sent["faction_general_name"] = None
        sent["knesset_faction_id"] = None
        return

    if person['factions_memberships']:
        for faction in person['factions_memberships']:
            faction_start_date = datetime.strptime(faction['start_date'], DATE_FORMAT)
            faction_end_date = datetime.strptime(faction['end_date'], DATE_FORMAT)
            if faction_start_date <= protocol_date <= faction_end_date and sent["knesset_number"] == faction["knesset_number"]:
                sent["faction_id"] = faction['faction_id']
                sent["faction_general_name"] = faction['faction_name']
                sent["knesset_faction_id"] = faction["knesset_faction_id"]
                break
        if "faction_id" not in sent:
            # if person['full_name'] not in known_and_checked_missing_factions: # return this when want to check if knesset members are not a mistake as a speaker in this protocol
            #     print(
            #         f"no match for a faction. date is {protocol_date}. person is:id: {person['person_id']} {person['full_name']}. factions are: {person['factions_memberships']}. sent id is: {sent['sentence_id']} in protocol: {sent['protocol_name']}")
            sent["faction_id"] = None
            sent["faction_general_name"] = None
            sent["knesset_faction_id"] = None
    else:
        sent["faction_id"] = None
        sent["faction_general_name"] = None
        sent["knesset_faction_id"] = None


def add_person_fields_to_sent(sent, person):
    sent["speaker_first_name"] = person["first_name"]
    sent["speaker_last_name"] = person["last_name"]
    sent["speaker_is_knesset_member"] = person['is_knesset_member']
    sent["speaker_gender"] = person["gender"]
    sent["speaker_email"] = person["email"]
    sent["speaker_last_updated_date"] = person['last_updated_date']
    sent["speaker_date_of_birth"] = person['date_of_birth']
    sent["speaker_place_of_birth"] = person['place_of_birth']
    sent['speaker_year_of_aliya'] = person['year_of_aliya']
    sent["speaker_date_of_death"] = person['date_of_death']
    sent["speaker_mother_tongue"] = person['mother_tongue']
    sent['speaker_religion'] = person['religion']
    sent['speaker_nationality'] = person['nationality']
    sent["speaker_religious_orientation"] = person['religious_orientation']
    sent["speaker_residence"] = person['residence']
    sent["speaker_factions_memberships"] = person['factions_memberships']
    sent["speaker_languages"] = person['languages']
    sent["speaker_sources"] = person['allSources']
    sent["speaker_notes"] = person["notes"]
    update_current_faction(sent, person)

def update_current_faction_name(sent, faction):
    protocol_date = datetime.strptime(sent["protocol_date"], DATE_FORMAT)
    faction_names = faction["other_names"]
    sent["current_faction_name"] = faction['faction_name']
    for name in faction_names:
        start_date = datetime.strptime(name['name_start_date'], DATE_FORMAT)
        end_date = datetime.strptime(name['name_end_date'], DATE_FORMAT)
        if start_date <= protocol_date <= end_date:
            sent["current_faction_name"] = name["name"]
            break

def update_coalition_membership(sent, faction):
    protocol_date = datetime.strptime(sent["protocol_date"], DATE_FORMAT)

    coalition_memberships = faction['coalition_or_opposition_memberships']
    if coalition_memberships:
        for membership in coalition_memberships:
            start_date = datetime.strptime(membership['start_date'], DATE_FORMAT)
            end_date = datetime.strptime(membership['end_date'], DATE_FORMAT)
            member_of_coalition = membership['member_of_coalition']
            if start_date <= protocol_date <= end_date:
                if member_of_coalition:
                    sent["member_of_coalition_or_opposition"] = "coalition"
                else:
                    sent["member_of_coalition_or_opposition"] = "opposition"
                break
        if "member_of_coalition_or_opposition" not in sent:
            # print(f"didn't match coalition/opposition phase.faction: {faction['faction_name']} protocol date: {protocol_date}, coalition memberships: {coalition_memberships}. sent id: {sent['sentence_id']} in protocol: {sent['protocol_name']}")
            sent["member_of_coalition_or_opposition"] = None
    else:
        print(f"faction with no coalition-opposition memberships {faction['faction_name']} faction id: {faction['faction_id']} in protocol: {sent['protocol_name']} speaker is: {sent['speaker_name']} date is {sent['protocol_date']}")
        sent["member_of_coalition_or_opposition"] = None


def add_faction_fields_to_sent(sent, faction):
    if faction:
        sent["faction_popular_initials"] = faction['faction_popular_initials']
        sent["faction_active_periods"] = faction['active_periods']
        sent["faction_knesset_numbers"] = faction["knesset_numbers"]
        sent["faction_coalition_or_opposition_memberships"] = faction['coalition_or_opposition_memberships']
        sent["faction_political_orientation"] = faction['political_orientation']
        sent["faction_other_names"] = faction['other_names']
        sent["faction_notes"] = faction["notes"]
        sent["faction_wiki_link"] = faction["wiki_link"]
        update_current_faction_name(sent, faction)
        update_coalition_membership(sent, faction)
    else:
        sent["faction_popular_initials"] = None
        sent["faction_active_periods"] = None
        sent["faction_knesset_numbers"] = None
        sent["faction_coalition_or_opposition_memberships"] = None
        sent["faction_political_orientation"] = None
        sent["faction_other_names"] = None
        sent["faction_notes"] = None
        sent["faction_wiki_link"] = None
        sent["current_faction_name"] = None
        sent["member_of_coalition"] = None



def get_knesset_members():
    knesset_members = {}
    with open(all_knesset_members_path, encoding="utf-8") as file:
        knesset_members_rows = file.readlines()
        for row in knesset_members_rows:
            km_json = json.loads(row)
            knesset_members[km_json["person_id"]] = km_json
    return knesset_members


def get_factions():
    factions = {}
    with open(factions_jsonl_file_path, encoding="utf-8") as file:
        factions_rows = file.readlines()
        for row in factions_rows:
            faction_json = json.loads(row)
            factions[faction_json["faction_id"]] = faction_json
    return factions


def add_all_fields_to_sent(sent, knesset_members, factions):
    knesset_member = knesset_members.get(sent["speaker_id"], None)
    if knesset_member:
        person = knesset_member
    else:
        split_str = sent['speaker_name'].split(maxsplit=1)
        if len(split_str) >1:
            first_name = split_str[0]
            last_name = split_str[1]
        else:
            if split_str:
                first_name = split_str
                last_name = ""
            else:
                first_name = ""
                last_name = ""
        person = Person(sent["speaker_id"], first_name, last_name)
        person.is_knesset_member = False
        person = person.__dict__
    add_person_fields_to_sent(sent, person)
    sent_faction = factions.get(sent["faction_id"], None)
    add_faction_fields_to_sent(sent, sent_faction)



def convert_protocols_to_full_fields_sentences(protocols_path, knesset_members, factions, output_path):
    if os.path.exists(output_path):
        os.remove(output_path)
    with open(protocols_path, encoding="utf-8") as file:
        for line in file:
            try:
                protocol_json_entity = json.loads(line)
            except:
                print(f"couldnt load json. line is: {line}")
                continue
            sentences = protocol_to_sentences(protocol_json_entity)
            for sent in sentences:
                add_all_fields_to_sent(sent, knesset_members, factions)
                with open(output_path, "a", encoding="utf-8") as output_file:
                    json_str = json.dumps(sent, ensure_ascii=False)
                    print(json_str, file=output_file)



if __name__ == '__main__':
    knesset_members = get_knesset_members()
    factions = get_factions()
    output_path = sentences_jsonl_files
    committees_output_path = os.path.join(output_path, "committee_full_sentences.jsonl")
    plenaries_output_path = os.path.join(output_path, "plenary_full_sentences.jsonl")
    convert_protocols_to_full_fields_sentences(plenaries_processed_data_jsonl_file_path, knesset_members, factions,
                                               plenaries_output_path)
    convert_protocols_to_full_fields_sentences(committees_processed_data_jsonl_file_path, knesset_members, factions, committees_output_path)

