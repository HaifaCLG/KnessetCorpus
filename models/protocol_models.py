import re
import uuid
from abc import ABC, abstractmethod

from Knesset_corpus_processing.aux_functions import *
from models.person_model import Person
from models.sentence_model import Sentence


import logging
logging.basicConfig(filename=log_file_path, level=logging.INFO,  format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S', encoding='utf-8')




class Protocol(ABC):
    def __init__(self, document_name, document):
        self.protocol_name = document_name# file name will serve as protocol id
        self.session_name = None
        self.document = document
        self.all_knesset_members_df = None
        self.file_name = document_name
        self.knesset_number = None
        self.protocol_number = None
        self.protocol_date = None
        self.is_ocr_output = False
        self.protocol_type = 'unknown'
        self.protocol_sentences = []


    @abstractmethod
    def set_protocol_records_list(self):
        pass

    def get_protocol_extracted_data_records(self):
        self.extract_info_from_protocol_document()
        self.set_protocol_records_list()
        return self.records_list

    def extract_knesset_number(self):
         return self.file_name.split('_')[0]

    def extract_protocol_date(self):
        protocol_date = None
        for par in self.document.paragraphs:
            if ('יום' in par.text) and any(month in par.text for month in MONTHS):
                protocol_date = par.text
                break
        return protocol_date
    def extract_protocol_date_version_2(self):
        protocol_date = None
        for par in self.document.paragraphs:
            if ('יום' in par.text) and ('שעה' in par.text) and year_in_text(par.text):
                protocol_date = par.text
                break
        return protocol_date

    def extract_protocol_number(self):
        self.protocol_number = None

    def extract_list_of_protocol_Sentences_and_speakers(self, version_2_flag = False, version_3_flag = False, version_4_flag = False, version_5_flag = False, version_6_flag = False, version_7_flag = False):
        protocol_speaker_names_and_matched_names_set =set()
        yor_string = 'stam'
        yor_string_flag = True
        IS_VOTE = False
        speakers_names = set()
        is_chairman = {}  # Dictionary: 'speaker_name':True/False
        last_speaker_text = ''
        turn_num = 0
        protocol_speakers = set()
        known_unvalid_speaker_names = set()
        self.check_validity_of_protocol()

        for par in self.document.paragraphs:
            if version_4_flag:
                if yor_string_flag and Protocol.is_yor_in_text(par.text):
                    yor_string = par.text
                    yor_string_flag = False
                    continue

            # I assume first speaker_name is always the chairman
            if len(speakers_names) == 0 and Protocol.paragraph_is_first_speaker(par, version_2_flag=version_2_flag, version_4_flag = version_4_flag, yor_string=yor_string, version_5_flag=version_5_flag):
                raw_speaker_name, speaker_name = self.get_raw_and_clean_speaker_name(par, version_3_flag,
                                                                                     version_5_flag, version_7_flag)

                person = self.get_speaker_valid_person(speaker_name, self.all_knesset_members_df, known_non_valid_speakers=known_unvalid_speaker_names)
                if not person:
                    continue
                if speaker_name not in known_unvalid_speaker_names:
                    list_row = (raw_speaker_name, person.full_name, person.is_knesset_member, self.protocol_name)
                    protocol_speaker_names_and_matched_names_set.add(list_row)
                    Protocol.update_is_chairman(is_chairman, person.person_id, True)
                    protocol_speakers.add(person)

                last_speaker = person
                speakers_names.add(speaker_name)
                continue
            if len(speakers_names) > 0:
                if Protocol.paragraph_is_a_speaker(par, version_2_flag, version_6_flag):
                    IS_VOTE = False
                    # update last speaker_name before continuing to next speaker_name
                    num_of_sentences_in_protocol_before_insert = len(self.protocol_sentences)
                    self.create_Sentences_from_text_and_add_to_protocol(is_chairman[last_speaker.person_id], last_speaker,
                                                                        last_speaker_text, turn_num, known_unvalid_speaker_names)
                    num_of_sentences_in_protocol_after_insert = len(self.protocol_sentences)
                    if num_of_sentences_in_protocol_after_insert > num_of_sentences_in_protocol_before_insert:
                        turn_num += 1


                    # update new speaker_name
                    raw_speaker_name, speaker_name = self.get_raw_and_clean_speaker_name(par, version_3_flag,
                                                                                         version_5_flag, version_7_flag)
                    matches_in_protocol_speakers = list(filter(lambda person:person.full_name == speaker_name, protocol_speakers))
                    if matches_in_protocol_speakers:
                        person = matches_in_protocol_speakers[0]#i assume only one match in single protocol
                    else:
                        try:
                            person = self.get_speaker_valid_person(speaker_name,self.all_knesset_members_df, known_non_valid_speakers=known_unvalid_speaker_names)
                        except Exception as e:
                            logging.error(
                                f"get_speaker_valid_person failed. exception was:{repr(e)}\n")
                            continue
                        if not person:
                            continue
                        if speaker_name not in known_unvalid_speaker_names:
                            list_row = (raw_speaker_name, person.full_name, person.is_knesset_member, self.protocol_name)
                            protocol_speaker_names_and_matched_names_set.add(list_row)
                        Protocol.update_is_chairman(is_chairman, person.person_id, False)
                        protocol_speakers.add(person)

                    last_speaker_text = ''
                    speakers_names.add(speaker_name)
                    last_speaker = person
                    continue
                else:
                    if Protocol.is_end_of_meeting(par.text):
                        self.create_Sentences_from_text_and_add_to_protocol(is_chairman[last_speaker.person_id],
                                                                            last_speaker,
                                                                            last_speaker_text, turn_num, known_unvalid_speaker_names)
                        break
                    elif Protocol.is_vote(par, IS_VOTE):
                        IS_VOTE = True  # this flag will stay true until a new speaker_name starts talking
                        last_speaker_text += par.text
                        last_speaker_text += " "

                    elif Protocol.is_new_topic(par) and last_speaker_text != EMPTY_STRING:
                        num_of_sentences_in_protocol_before_insert = len(self.protocol_sentences)
                        self.create_Sentences_from_text_and_add_to_protocol(is_chairman[last_speaker.person_id],
                                                                            last_speaker,
                                                                            last_speaker_text, turn_num, known_unvalid_speaker_names)
                        num_of_sentences_in_protocol_after_insert = len(self.protocol_sentences)
                        if num_of_sentences_in_protocol_after_insert > num_of_sentences_in_protocol_before_insert:
                            turn_num += 1
                        last_speaker_text = ""

                    else:
                        if par.text == EMPTY_STRING:
                            if last_speaker_text == EMPTY_STRING:
                                continue
                            else:
                                last_speaker_text += '    '  # 4 spaces instaed of new line \n so this won't be a problem when writing to csv file. in case you want to split to paragraphs
                                continue
                        last_speaker_text += par.text
                        last_speaker_text += " "
        if len(protocol_speakers) == 0:
            raise Exception(f"could not find first speaker in {self.file_name}\n")
        protocol_speakers_path = os.path.join(people_jsons_by_protocol_path, self.protocol_name+'.jsonl')
        write_list_of_class_subjects_to_json_lines_file(protocol_speakers, protocol_speakers_path)
        columns = ['original_speaker_name', 'assigned_name', 'found_match', 'protocol_name']
        matched_names_path = os.path.join(processed_knesset_data_path,"protocol_matched_names","matched_names_protocols")
        write_records_to_csv(list(protocol_speaker_names_and_matched_names_set), os.path.join(matched_names_path,self.protocol_name+'.csv'), columns)
        return

    def get_raw_and_clean_speaker_name(self, par, version_3_flag, version_5_flag, version_7_flag):
        raw_speaker_name = par.text.split(':')[0]
        speaker_name = get_clean_speaker_name(raw_speaker_name, version_3_flag, version_5_flag, version_7_flag)
        if not speaker_name:
            raise Exception(f"speaker name is empty {raw_speaker_name}\n")
        return raw_speaker_name, speaker_name

    def check_validity_of_protocol(self):
        if len(self.document.paragraphs) < 30:
            print(f" {self.file_name} is too short!")
            raise Exception(f" {self.file_name} is too short!\n")

        all_empty_strings = True
        for par in self.document.paragraphs:
            if par.text.strip() != EMPTY_STRING:
                all_empty_strings = False
                break
        if all_empty_strings:
            raise Exception(f" {self.file_name} protocol is empty!\n")

    def create_Sentences_from_text_and_add_to_protocol(self, is_chairman:bool, last_speaker, last_speaker_text,
                                                       turn_num, unvalid_speakers):
        try:
            sents = split_text_to_sentences(last_speaker_text)
        except Exception as e:
            logging.error(f"split_text_to_sentences failed. exception was:{repr(e)}\n last speaker text was: {last_speaker_text}\n speaker name was : {last_speaker}")
            raise Exception(f'file name {self.file_name} couldnt split sentences\n')

        if last_speaker_text.strip() and not sents:
            logging.info(f'file name {self.file_name} couldnt split sentences\n')
            if PRINT_ALL:
                print(f'file name {self.file_name} couldnt split sentences')
            raise Exception(f'file name {self.file_name} couldnt split sentences\n')
        sent_counter = 0
        for sent_text in sents:
            sent_id = str(uuid.uuid4())
            sent = Sentence(sent_id, sent_text, self.protocol_name, turn_num, sent_counter)
            sent.speaker_id = last_speaker.person_id
            sent.speaker_name = last_speaker.full_name
            sent.is_chairman = is_chairman
            if last_speaker.first_name in unvalid_speakers:
                sent.is_valid_speaker = False
            self.protocol_sentences.append(sent)
            sent_counter += 1




    def extract_info_from_protocol_document(self):
        self.knesset_number = self.extract_knesset_number()
        self.protocol_number = self.extract_protocol_number()
        # self.protocol_date = self.extract_protocol_date()#I extract date separately
        try:
            self.all_knesset_members_df = read_json_lines_file_to_df(all_knesset_members_path)
        except Exception as e:
            logging.error(f"read_json_lines_file_to_df failed. exception was:{repr(e)}\n")
            raise e
        try:
            self.extract_list_of_protocol_Sentences_and_speakers(version_2_flag=False,version_3_flag=False,version_4_flag=False, version_5_flag=False, version_6_flag=False, version_7_flag=False)#TODO remove this flag
        except Exception as e:
            logging.error(f"extract_list_of_protocol_Sentences_and_speakers failed. exception was:{repr(e)}\n")
            raise e
        # self.protocol_speakers_and_their_text = self.extract_list_of_protocol_speakers_and_their_text()


    '''protocol paragraph util functions'''

    @staticmethod
    def is_yor_in_text(text):
        if ('יו"ר' in text) or ('יו״ר' in text) or ('יו”ר' in text) or ('יושב ראש' in text) or ('יושב הראש' in text) or ('יושב-ראש' in text):
            return True
    @staticmethod
    def paragraph_is_first_speaker(paragraph, version_2_flag = False, version_4_flag =False, yor_string = 'temp', version_5_flag = False):
        if version_5_flag:
            if Protocol.is_yor_in_text(paragraph.text):
                if (':' in paragraph.text):
                    if 'ועדת הכנסת' in paragraph.text or 'ועדה' in paragraph.text:
                        return True
        if version_4_flag:
            if (':' in paragraph.text):
                name_text = paragraph.text.split(':')[0]
                if name_text:
                    if name_text in yor_string or yor_string in name_text:
                        return True
        if version_2_flag:
            if (':' in paragraph.text):
                name_text = paragraph.text.split(':')[0]
                if Protocol.is_yor_in_text(name_text):
                    return True
        else:
            return ((Protocol.is_yor_in_text(paragraph.text)) and (':' in paragraph.text) and ('שעה' not in paragraph.text) and (paragraph.runs[0].underline == True or (
                paragraph.style.name != 'Normal' and 'Table' not in paragraph.style.name)) and (
                       paragraph.runs[0].bold == None or paragraph.runs[0].bold == False))
        return False

    @staticmethod
    def paragraph_is_a_speaker(paragraph, version_2_flag= False, version_6_flag = False):
        if version_2_flag:
            if ':' in paragraph.text:
                name_text =  paragraph.text.split(':')[0]
                if (len(re.sub(r'\<<[^)]*\>>', '', name_text).split())<= MAX_SPEAKER_NAME_LENGTH):
                    if not Protocol.is_end_of_meeting(paragraph.text):
                        return True
        elif version_6_flag:
            return (':' in paragraph.text) and (
                        len(re.sub(r'\<<[^)]*\>>', '', paragraph.text).split()) <= MAX_SPEAKER_NAME_LENGTH) and (
                               paragraph.runs[0].underline == True or (
                               paragraph.style.name != 'Normal' and 'Table' not in paragraph.style.name)) and not Protocol.is_end_of_meeting(paragraph.text)
        else:
            return (':' in paragraph.text) and (len(re.sub(r'\<<[^)]*\>>', '', paragraph.text).split())<= MAX_SPEAKER_NAME_LENGTH) and (paragraph.runs[0].underline == True or (
                    paragraph.style.name != 'Normal' and 'Table' not in paragraph.style.name)) and (
                           paragraph.runs[0].bold == None or paragraph.runs[0].bold == False) and not Protocol.is_end_of_meeting(paragraph.text)
        return False

    @staticmethod
    def is_end_of_meeting(text):
        return 'הישיבה ננעלה' in text

    @staticmethod
    def is_new_topic(paragraph):
        return (paragraph.text != EMPTY_STRING and (paragraph.alignment == 'CENTER' or (
                paragraph.style.name != 'Normal' and 'Table' not in paragraph.style.name and 'יור' not in paragraph.style.name)))

    @staticmethod
    def is_vote(paragraph, IS_VOTE_FLAG=False):
        return False
        # return (IS_VOTE_FLAG or 'הצבעה' in paragraph.text and 'מס' in paragraph.text and Protocol.is_new_topic(paragraph))

    @staticmethod
    def remove_knesset_member_from_speaker_name(speaker):
        if 'ח"כ' in speaker:
            knesset_acronyms = 'ח"כ'
        elif 'ח״כ' in speaker:
            knesset_acronyms = 'ח״כ'
        elif 'חה"כ' in speaker:
            knesset_acronyms = 'חה"כ'
        elif 'חה״כ' in speaker:
            knesset_acronyms = 'חה״כ'
        elif 'כנסת' in speaker:
            knesset_acronyms = 'כנסת'
        else:
            return speaker
        speaker = speaker.split(knesset_acronyms)[1].strip()
        return speaker

    @staticmethod
    def remove_minister_from_speaker_name(speaker):
        if 'שר ' in speaker:
            name = speaker.split('שר',1)[1]
            speaker = name
        return speaker


    @staticmethod
    def update_chairman_speaker_name(speaker, version_3_flag = False, version_5_flag = False):
        if 'יו"ר' in speaker:
            yor = 'יו"ר'
        elif 'יו״ר' in speaker:
            yor = 'יו״ר'
        elif 'יו”ר' in speaker:
            yor = 'יו”ר'
        elif 'יושב ראש' in speaker:
            yor = 'יושב ראש'
        elif 'יושב הראש' in speaker:
            yor = 'יושב הראש'
        else:
            return speaker
        if version_5_flag:
            if 'ועדת' in speaker or 'ועדה' in speaker:
                speaker = speaker.split(yor)[0]
            else:
                speaker = speaker.split(yor)[1].strip()
        elif version_3_flag:
            if speaker.split(yor)[1].strip() == EMPTY_STRING:
                speaker = speaker.split(yor)[0].strip()
        else:
            speaker = speaker.split(yor)[1].strip()
        return speaker

    @staticmethod
    def update_is_chairman(is_chairman_dict, speaker, is_chairman: bool):
        if is_chairman_dict.get(speaker, -1) == -1:
            is_chairman_dict[speaker] = is_chairman

    @staticmethod
    def update_is_chairman(is_chairman_dict, speaker_id, is_chairman: bool):
        if is_chairman_dict.get(speaker_id, -1) == -1:
            is_chairman_dict[speaker_id] = is_chairman

    @staticmethod
    def update_speaker_and_is_chairman_accordingly(is_chairman, speaker):
        if 'יו"ר' in speaker:
            speaker = speaker.split('יו"ר')[1]
            is_chairman[speaker] = True
        else:
            is_chairman[speaker] = False
        return speaker

    def get_speaker_valid_person(self, speaker_name, knesset_members_df ,known_non_valid_speakers=None):
        if known_non_valid_speakers and speaker_name in known_non_valid_speakers:
            person_id = str(uuid.uuid4())
            person = Person(person_id, speaker_name, '')
            person.is_knesset_member = False
            return person

        found_match = False
        knesset_members_names = knesset_members_df.loc[:, 'full_name']
        if 'כהן' in speaker_name or 'לוי' in speaker_name:
            best_matches_indexes = get_close_matches_indexes(speaker_name, knesset_members_names, cutoff=0.9)
        else:
            best_matches_indexes = get_close_matches_indexes(speaker_name, knesset_members_names, cutoff=0.75)

        if len(speaker_name.split())>3 and not best_matches_indexes:
            while len(speaker_name.split())>=3:
                speaker_name = speaker_name.split(' ',1)[1]
                if 'כהן' in speaker_name or 'לוי' in speaker_name:
                    best_matches_indexes = get_close_matches_indexes(speaker_name, knesset_members_names, cutoff=0.9)
                else:
                    best_matches_indexes = get_close_matches_indexes(speaker_name, knesset_members_names, cutoff=0.75)
                if best_matches_indexes:
                    break

        for idx in best_matches_indexes:
            member_row = knesset_members_df.iloc[[idx]]
            for faction in list(member_row.factions_memberships)[0]:
                if faction['knesset_number'] == self.knesset_number:
                    found_match = True
                    break
            if found_match:
                break
        if found_match:
            person = create_person_object_from_member_row(member_row)
        else:
            person_id = str(uuid.uuid4())
            split_str = speaker_name.split(maxsplit=1)
            if len(split_str) < 2:
                if known_non_valid_speakers != None:
                    known_non_valid_speakers.add(speaker_name)
                logging.info(f'Error: in file {self.file_name}: speaker name had only one word. speaker will not be saved in dataset. speaker name is: {str(speaker_name)}\n')
                person = Person(person_id,speaker_name,'')
                person.is_knesset_member = False

            else:
                first_name = split_str[0]
                last_name = split_str[1]
                person = Person(person_id, first_name, last_name)
                person.is_knesset_member = False
        return person



class Committee_Protocol(Protocol):
    def __init__(self, protocol_file_path, document):
        super().__init__(protocol_file_path, document)
        self.session_name = None
        # self.protocol_topic = None
        self.protocol_type = 'committee'

    def extract_info_from_protocol_document(self):
        super().extract_info_from_protocol_document()
        self.session_name = self.extract_protocol_committee_name()
        # self.protocol_topic = self.extract_protocol_topic()

    def extract_protocol_number(self):
        protocol_number = None
        for par in self.document.paragraphs:
            if 'פרוטוקול מס' in par.text:
                try:
                    protocol_number = par.text.split()[2]
                    break
                except:
                    protocol_number = None
                    break
        return protocol_number

    def extract_protocol_committee_name(self):
        committee_name = None
        for par in self.document.paragraphs:
            if 'מישיבת' in par.text:
                split_token = 'מישיבת'
            elif 'מהישיבה' in par.text:
                split_token = 'מהישיבה'
            else:
                continue
            committee_name = par.text.split(split_token)[1]
            break
        return committee_name

    def extract_protocol_topic(self):
        topic = ''
        topic_tokens = ['סדר היום:', 'סדר-היום:']
        for par in self.document.paragraphs:
            if any(token in par.text for token in topic_tokens):
                topic += par.text
                continue
            if topic:
                if "נכחו" in par.text or "נוכחים" in par.text:
                    break
                else:
                    topic += par.text
        if topic == EMPTY_STRING:
            fixed_topic = None
        else:
            for token in topic_tokens:
                if token in topic:
                    topic_token = token
                    break
            assert topic_token, 'Error: How did we get here? one of the topic_tokens were supposed to be in the topic!'
            fixed_topic = topic.split(topic_token)[1]
        return fixed_topic

    def set_protocol_records_list(self):
        committee_records = []
        for tuple in self.protocol_speakers_and_their_text:
            row = list(tuple)
            row.append(self.knesset_number)
            row.append(self.protocol_number)
            row.append(self.committee_name)
            row.append(self.protocol_date)
            row.append(self.protocol_topic)
            row.append(self.protocol_topic)
            row.append(self.file_name)
            row.append((self.is_ocr_output))
            committee_records.append(row)
        self.records_list = committee_records



class Plenary_Protocol(Protocol):
    def __init__(self, protocol_file_path, document):
        super().__init__(protocol_file_path, document)
        self.protocol_type = 'plenary'
        self.session_name = 'ישיבת מליאה'

    def extract_protocol_number(self):
        protocol_number = None
        for par in self.document.paragraphs:
            if "הישיבה" in par.text and par.runs[0].bold == True or 'הישיבה ה' in par.text:
                protocol_number = par.text
                break
        return protocol_number

    def extract_info_from_protocol_document(self):
        super().extract_info_from_protocol_document()


    def set_protocol_records_list(self):
        plenary_records = []
        for tuple in self.protocol_speakers_and_their_text:
            row = list(tuple)
            row.append(self.knesset_number)
            row.append(self.protocol_number)
            row.append(self.protocol_date)
            row.append(self.file_name)
            row.append((self.is_ocr_output))
            plenary_records.append(row)
        self.records_list = plenary_records



def year_in_text(text):
    years = range(1948,2024)
    for year in years:
        if str(year) in text or (str(year))[-2:] in text:
            return True
    return False



def get_clean_speaker_name(raw_speaker_name, version_3_flag = False, version_5_flag = False,version_7_flag = False):
    speaker_name = raw_speaker_name
    if not version_5_flag:
        speaker_name = re.sub(r'\<<[^)]*\>>', '', raw_speaker_name)
        speaker_name = Protocol.remove_knesset_member_from_speaker_name(speaker_name)
    if version_7_flag:
        speaker_name = re.sub(r'\<<[^)]*\>>', '', raw_speaker_name)
        speaker_name = re.sub(r'\([^)]*\)', '', speaker_name)
        return speaker_name

    speaker_name = Protocol.remove_minister_from_speaker_name(speaker_name)
    speaker_name = Protocol.update_chairman_speaker_name(speaker_name, version_3_flag, version_5_flag)
    speaker_name = re.sub(r'\([^)]*\)', '', speaker_name)
    speaker_name = speaker_name.strip()
    return speaker_name


def create_person_object_from_member_row(member):
    person = Person(str(member.person_id.values[0]), str(member.first_name.values[0]), str(member.last_name.values[0]))
    member_dict = dict(member)
    for key in member_dict.keys():
        if key == 'person_id':
            continue
        if(key == 'is_knesset_member' or key == 'is_current'):
            person.__dict__[key] = bool(member[key].values[0])
        else:
            person.__dict__[key] = member[key].values[0]

    return person
