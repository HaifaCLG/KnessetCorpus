
class Person():
    def __init__(self, person_id, first_name, last_name):
        self.person_id = person_id
        self.first_name = first_name
        self.last_name = last_name
        self.full_name = f'{first_name} {last_name}'.strip()
        self.is_knesset_member = None
        self.gender = None
        self.email = None
        self.is_current = None
        self.last_updated_date = None
        self.date_of_birth = None
        self.place_of_birth = None
        self.year_of_aliya = None
        self.date_of_death = None
        self.mother_tongue = None
        self.religion = None
        self.nationality = None
        self.religious_orientation = None
        self.residence = None
        self.factions_memberships = []
        self.languages = []
        self.allSources = []
        self.wikiLink = None
        self.notes = []

    def __eq__(self, other):
        return self.person_id == other.person_id
    def __hash__(self):
        return hash(self.person_id)

    def add_faction(self,general_faction_id, faction_id, faction_name, knesset_number,start_date, end_date ):
        if not start_date:
            start_date = None
        if not end_date:
            end_date = None

        faction_membership_dict = {'faction_id':general_faction_id,'knesset_faction_id':faction_id, 'faction_name':faction_name, 'knesset_number':knesset_number, 'start_date':start_date, 'end_date':end_date}
        self.factions_memberships.append(faction_membership_dict)



