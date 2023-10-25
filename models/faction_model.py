from Knesset_corpus_processing.params_config import DATE_FORMAT
from Knesset_corpus_processing.process_factions_and_knesset_members_files import change_date_format


class Faction():
    def __init__(self, faction_name, faction_id):
        self.faction_name = faction_name
        self.faction_popular_initials = ""
        self.faction_id = faction_id
        self.active_periods = []
        self.knesset_numbers = []
        self.coalition_or_opposition_memberships = []
        self.political_orientation = ""
        self.other_names = []
        self.notes = ""
        self.wiki_link = ""

    def faction_insert_other_name(self,name, name_start_date, name_end_date):
        current_date_format = "%d/%m/%Y"
        try:
            name_start_date = change_date_format(name_start_date, current_date_format, DATE_FORMAT)
        except:
            print(f'in {name} found date is: {name_start_date} does not match format')
            name_start_date = None
        try:
            name_end_date = change_date_format(name_end_date, current_date_format, DATE_FORMAT)
        except:
            print(f'in {name} found date is: {name_end_date} does not match format')
            name_end_date = None

        other_name_record = {"name":name, "name_start_date":name_start_date, "name_end_date":name_end_date}
        self.other_names.append(other_name_record)

    def faction_add_knesset_number(self, knesset_num):
        if len(knesset_num)==1:
            knesset_num = f'0{knesset_num}'
        self.knesset_numbers.append(knesset_num)
    def faction_add_date_period(self, start_date, end_date):
        self.active_periods.append({"start_date":start_date, "end_date":end_date})

    def faction_add_coalition_membership(self, knesset_num, start_date, end_date, knesset_faction_name, member_of_coalition, notes):
        if len(knesset_num) == 1:
            knesset_num = f'0{knesset_num}'
        coalition_membership = {"knesset_num":knesset_num, "start_date":start_date, "end_date":end_date, "knesset_faction_name":knesset_faction_name, "member_of_coalition":member_of_coalition, "notes":notes}
        self.coalition_or_opposition_memberships.append(coalition_membership)

