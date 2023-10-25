from datetime import datetime
from aux_functions import write_dictionary_values_to_json_lines_file, write_list_of_class_subjects_to_json_lines_file
from params_config import *
import pandas as pd
from models.person_model import Person
from models.faction_model import *

knesset_members_jsonl_output_file_path = all_knesset_members_path


def create_faction_name_to_id_dict():
    with open(FactionMembers_file_path, encoding='utf-8') as file:
        factionMembers_df = pd.read_csv(file,keep_default_na=False)
    faction_name_to_id_dict = {}
    for index, row in factionMembers_df.iterrows():
        faction_name = row.Faction.strip()
        faction_id = row.FactionID.strip()
        if faction_name and faction_id:
            if faction_name in faction_name_to_id_dict:
                if faction_id != faction_name_to_id_dict[faction_name]:
                    print('error! same faction with different ids! faction name: ' + str(faction_name) + ' faction id 1: ' + str(faction_id) +' faction id 2: ' + str(faction_name_to_id_dict[faction_name]))
                    continue
                else:
                    continue
            else:
                faction_name_to_id_dict[faction_name]=faction_id
    return faction_name_to_id_dict

def process_KnessetMembers_file_and_create_person_instances(KnessetMembers_file_path):
    with open(KnessetMembers_file_path, encoding='utf-8') as file:
        knesset_members_df = pd.read_csv(file, keep_default_na=False)
    knesset_members_df = knesset_members_df.reset_index()  # make sure indexes pair with number of rows
    knesset_members_person_dict = {}
    for index, row in knesset_members_df.iterrows():
        person_id = str(row.PersonID)
        first_name = row.FirstName
        last_name = row.LastName
        person = Person(person_id, first_name, last_name)
        person.is_knesset_member = True
        gender_heb = row.GenderDesc
        if gender_heb.strip() == "נקבה":
            gender = "female"
        elif gender_heb.strip() == "זכר":
            gender = "male"
        else:
            print(f"{person_id}, {row['שם פרטי']}, {row['שם משפחה']} has illegal gender: {gender_heb}")
            gender = gender_heb
        person.gender = gender
        person.email = row.Email
        person.is_current = row.IsCurrent
        if row.LastUpdatedDate:
            person.last_updated_date = row.LastUpdatedDate
        else:
            person.last_updated_date = None
        if row.DateOfBirth:
            person.date_of_birth = row.DateOfBirth
        else:
            person.date_of_birth = None
        person.place_of_birth = row.PlaceOfBirth
        person.year_of_aliya = row.YearOfAliyah
        if row.DateOfDeath:
            person.date_of_death = row.DateOfDeath
        else:
            person.date_of_death = None
        person.mother_tongue = row.MotherTongue
        person.religion = row.Religion
        person.nationality = row.Nationality
        person.religious_orientation = row.ReligiousOrientation
        person.residence = row.Residence
        person.allSources.append(row.Sources)
        person.notes.append(row.Notes)
        person.notes = list(set(person.notes))

        knesset_members_person_dict[person_id] = person
    return knesset_members_person_dict


def process_FactionMembers_file_and_update_person_instances(FactionMembers_file_path, knesset_members_person_dict):
    with open(FactionMembers_file_path, encoding='utf-8') as file:
        faction_members_df = pd.read_csv(file, keep_default_na=False)
    faction_members_df = faction_members_df.reset_index()  # make sure indexes pair with number of rows
    for index, row in faction_members_df.iterrows():
        knesset_number = row.KnessetNum.strip()
        if row.KnessetNum and int(row.KnessetNum.strip()) == 0:
            continue
        person_id = row.PersonID.strip()
        if not person_id:
            continue
        elif person_id not in knesset_members_person_dict:
            print(str(person_id) + ' ' + str(row.FirstName) + ' ' + str(row.LastName) + ' did not exist in dict', flush=True)
            person = Person(row.PersonID, row.FirstName, row.LastName)
            knesset_members_person_dict[person_id] = person
            knesset_members_person_dict[person_id].is_knesset_member = True

        knesset_members_person_dict[person_id].add_faction(row.FactionID.strip(), row.Faction.strip(), knesset_number, row.StartDate, row.FinishDate)
        sources = knesset_members_person_dict[person_id].allSources
        sources.append(row.OpenKnessetLink)
        sources.append(row.WikiLink)
        sources.append(row.KnessetLink)
        sources = list(set(sources))
        knesset_members_person_dict[person_id].allSources = sources
        knesset_members_person_dict[person_id].wikiLink = row.WikiLink

    print('Finished processing FactionMembers file', flush=True)

def get_new_faction_names(all_knesset_members_meta_data_file_path, faction_name_to_id_dict):
    all_new_factions = set()
    with open(all_knesset_members_meta_data_file_path, encoding='utf-8') as file:
        df = pd.read_csv(file, keep_default_na=False)
    for idx, row in df.iterrows():
        faction_name = row['סיעה'].strip()
        if faction_name not in faction_name_to_id_dict:
            all_new_factions.add(faction_name)
    all_new_factions_list = list(all_new_factions)
    all_new_factions_list.sort()
    print(all_new_factions_list.__sizeof__())
    for item in all_new_factions_list:
        if item:
            print(item)


def process_all_knesset_members_meta_data_file(all_knesset_members_meta_data_file_path, knesset_members_person_dict):
    with open(all_knesset_members_meta_data_file_path, encoding='utf-8') as file:
        all_kneeset_file_members = pd.read_csv(file, keep_default_na=False)
    all_kneeset_file_members = all_kneeset_file_members.reset_index()  # make sure indexes pair with number of rows

    for index, row in all_kneeset_file_members.iterrows():
        if row['כנסת'] and int(row['כנסת'].strip()) == 0:
            continue
        person_id = str(row.PersonID)
        if not person_id:
            continue
        if person_id not in knesset_members_person_dict:
            error_str = str(person_id)  + ' did not exist in dict'
            print(error_str)
            person = Person(person_id, row['שם פרטי'], row['שם משפחה'])
            knesset_members_person_dict[person_id] = person
            knesset_members_person_dict[person_id].is_knesset_member=True
            gender_heb = row['מין']
            if gender_heb.strip() == "נקבה":
                gender = "female"
            elif gender_heb.strip() == "זכר":
                gender = "male"
            else:
                print(f"{person_id}, {row['שם פרטי']}, {row['שם משפחה']} has illegal gender: {gender_heb}")
                gender = gender_heb
            knesset_members_person_dict[person_id].gender = gender
            knesset_members_person_dict[person_id].email = row['אימייל']
            knesset_members_person_dict[person_id].date_of_birth = row['תאריך לידה']
            knesset_members_person_dict[person_id].place_of_birth = row['מקום לידה']
            knesset_members_person_dict[person_id].year_of_aliya = row['שנת עלייה']
            knesset_members_person_dict[person_id].date_of_death = row['שנת פטירה']
            knesset_members_person_dict[person_id].residence = row['עיר מגורים']
            knesset_members_person_dict[person_id].mother_tongue =row['שפות'].split(',')[0]
        faction_name = row['שם מפלגה אחיד'].strip()
        knesset_number = row['כנסת']
        start_date = row['תאריך התחלה']
        end_date = row['תאריך סיום']
        knesset_faction_id = row['FactionID']
        general_faction_id = row['General_ID']
        knesset_members_person_dict[person_id].add_faction(general_faction_id, knesset_faction_id, faction_name, knesset_number,start_date, end_date)
        person_languages = row['שפות'].split(',')
        languages = knesset_members_person_dict[person_id].languages
        languages.extend(person_languages)
        all_known_languages = list(set(languages))
        knesset_members_person_dict[person_id].languages=all_known_languages

    print('Finished processing all_knesset_members_meta_data file', flush=True)





def change_date_format(date_str, current_format, wanted_format):
    datetime_object = datetime.strptime(date_str, current_format)
    new_datetime_str = datetime_object.strftime(wanted_format)
    return new_datetime_str

def process_factions_file(factions_file):
    factions = []
    with open(factions_file, encoding='utf-8') as file:
        factions_df = pd.read_csv(file,keep_default_na=False)
    for index, row in factions_df.iterrows():
        faction_name = row['שם המפלגה']
        faction_id = str(row['General_ID'])
        faction = Faction(faction_name, faction_id)
        start_dates = row["התחלת פעילות בכנסת"].split('\n')
        faction_start_dates = []
        faction_end_dates = []
        current_date_format = "%d/%m/%Y"
        for start_date in start_dates:
            try:
                faction_start_date = change_date_format(start_date, current_date_format, DATE_FORMAT)
                faction_start_dates.append(faction_start_date)
            except:
                print(f'in {faction_name} found date is: {start_date} does not match format')
                faction.start_date = None
        end_dates = row["סיום פעילות בכנסת"].split("\n")
        for end_date in end_dates:
            try:
                faction_end_date = change_date_format(end_date, current_date_format, DATE_FORMAT)
                faction_end_dates.append(faction_end_date)

            except:
                print(f'in {faction_name} found date is: {end_date} does not match format')
                faction.end_date = None
        for i, start_date in enumerate(faction_start_dates):
            faction.faction_add_date_period(start_date, faction_end_dates[i])
        knesset_numbers = row["כנסת"].split(",")
        for num in knesset_numbers:
            faction.faction_add_knesset_number(num)
        faction.political_orientation = row["קטגוריה פוליטית"]
        other_names = row["שמות אחרים"].split("\n")
        start_dates_other_names = row["תאריך התחלה לשם אחר"].split("\n")
        end_dates_other_names = row["תאריך סיום לשם אחר"].split("\n")
        for i, name in enumerate(other_names):
            if name:
                try:
                    faction.faction_insert_other_name(name, start_dates_other_names[i], end_dates_other_names[i])
                except Exception as e:
                    print(f'in faction {faction_name} couldnt insert other name: {name}. exception was: {e}')
                    continue
        faction.notes = row["הערות"]
        faction.wiki_link = row["ויקיפדיה"]
        factions.append(faction)
    return factions


def update_dates_format_for_all_fields(knesset_members_person_dict):
    current_date_format = "%d/%m/%Y"
    for id in knesset_members_person_dict.keys():
        person = knesset_members_person_dict[id]
        if person.date_of_birth:
            try:
                person.date_of_birth = change_date_format(person.date_of_birth,current_date_format,DATE_FORMAT)
            except Exception as e:
                print(e)
                print(f' in person: {person.person_id} {person.full_name} date of birth is: {person.date_of_birth}')
                continue
        else:
            person.date_of_birth = None
        if person.date_of_death:
            try:
                person.date_of_death=  change_date_format(person.date_of_death,current_date_format,DATE_FORMAT)
            except Exception as e:
                print(e)
                print(f' in person: {person.person_id} {person.full_name} date_of_death is {person.date_of_death}')
                continue
        else:
            person.date_of_death = None
        if person.last_updated_date:
            new_current_format = "%d/%m/%Y %H:%M"
            try:
                person.last_updated_date =  change_date_format(person.last_updated_date,new_current_format,DATE_FORMAT)
            except Exception as e:
                print(e)
                print(f' in person: {person.person_id} {person.full_name} last_updated_date is {person.last_updated_date}')
                continue
        else:
            person.last_updated_date = None

        for faction in person.factions_memberships:
            if faction['start_date'] :
                try:
                    faction['start_date'] =  change_date_format(faction['start_date'],current_date_format,DATE_FORMAT)
                except Exception as e:
                    print(e)
                    print(f" in person: {person.person_id} {person.full_name} faction start date: {faction['start_date']}")
                    continue
            else:
                faction['start_date'] = None
            if faction['end_date']:
                try:
                    faction['end_date'] =  change_date_format(faction['end_date'],current_date_format,DATE_FORMAT)
                except Exception as e:
                    print(e)
                    print(f" in person: {person.person_id} {person.full_name} faction end_date is {faction['end_date']}")
                    continue
            else:
                faction['end_date'] = None


def process_coalition_file(factions, coalition_file_path):
    with open(coalition_file_path, encoding="utf-8") as file:
        df = pd.read_csv(file,keep_default_na=False)
    for index, row in df.iterrows():
        faction_name = row['שם המפלגה הכללי']
        faction_id = str(row['General_ID'])
        knesset_num = str(row['כנסת'])
        current_name = row['שם המפלגה באותה כנסת']
        coalition = row['שיוך (קואליציה/אופוזיציה)']
        if coalition == "קואליציה":
            coalition_membership = True
        elif coalition == "אופוזיציה":
            coalition_membership = False
        else:
            print(f"in faction id: {faction_id} in knesset: {knesset_num}- wrong coalition name: {coalition}")
            continue

        start_date = row['תאריך התחלת חברות בקואליציה/אופוזיציה בכנסת']
        end_date = row['תאריך סיום חברות בקואליציה/אופוזיציה בכנסת']
        current_date_format = "%d/%m/%Y"
        try:
            coalition_membership_start_date = change_date_format(start_date, current_date_format, DATE_FORMAT)
        except:
            print(f'in {faction_name} in knesset: {knesset_num} found date is: {start_date} does not match format')
            faction.start_date = None
        try:
            coalition_membership_end_date = change_date_format(end_date, current_date_format, DATE_FORMAT)
        except:
            print(f'in {faction_name} in knesset: {knesset_num} found date is: {end_date} does not match format')
            faction.end_date = None
        notes = row['הערות']
        for faction in factions:
            if faction.faction_id == faction_id and faction.faction_name == faction_name:
                faction.faction_add_coalition_membership(knesset_num, coalition_membership_start_date, coalition_membership_end_date, current_name, coalition_membership, notes)
                break


if __name__ == '__main__':
    knesset_members_person_dict = process_KnessetMembers_file_and_create_person_instances(KnessetMembers_file_path)
    process_all_knesset_members_meta_data_file(all_knesset_members_meta_data_file_path, knesset_members_person_dict)
    update_dates_format_for_all_fields(knesset_members_person_dict)
    write_dictionary_values_to_json_lines_file(knesset_members_person_dict, knesset_members_jsonl_output_file_path)

    factions = process_factions_file(factions_file)
    process_coalition_file(factions, coalition_membership_file_path)
    write_list_of_class_subjects_to_json_lines_file(factions, factions_jsonl_file_path)

