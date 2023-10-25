import os
############FLAGS############
ONLY_UNSUCCESFUL = False
RETRY_UNSUCCESFUL = True
USE_MINI_KNESSET_CORPUS = False
IGNORE_EXISTING_PROTOCOL_OUTPUT_FILES = True
RUN_SEPARATE_FILES_INTO_Plenaries_AND_COMMITTEES = False
MULTIPROCESSING = True
NUMBER_OF_PROCESSES = 8
PRINT_ALL = False

###########PATHS#################
data_path = "<PATH_TO_DATA>"

##RAW DATA PATHS##
raw_knesset_data_path = os.path.join(data_path,"Knesset","all_knesset_data")
all_doc_protocols_path = os.path.join(raw_knesset_data_path,"protocols")
plenaries_protocols_doc_files_path = os.path.join(all_doc_protocols_path, "plenary_sessions")
committees_protocols_doc_files_path = os.path.join(all_doc_protocols_path, "committee_deliberations")
valid_plenary_protocols_doc_files_path = os.path.join(plenaries_protocols_doc_files_path, "valid_knessets")
valid_committee_protocols_doc_files_path = os.path.join(committees_protocols_doc_files_path, "valid_knessets")

#META DATA PATHS#
meta_data_files_path = os.path.join(data_path,"Knesset", "meta_data")
all_knesset_members_meta_data_file_path = os.path.join(meta_data_files_path,"all_knesset_members_personal_and_factions_data.csv")
factions_file = os.path.join(meta_data_files_path,"factions_list.csv")
KnessetMembers_file_path = os.path.join(meta_data_files_path, "knesset_members_personal_data.csv")
FactionMembers_file_path = os.path.join(meta_data_files_path, "knesset_members_factions_data.csv")
coalition_membership_file_path = os.path.join(meta_data_files_path, "factions_coalition_opposition_membership.csv")

##PROCESSED DATA PATHS##
processed_knesset_data_path = os.path.join(data_path,"processed_knesset")
docx_files_path  = os.path.join(processed_knesset_data_path, "docx_files")
knessets_jsonl_file_path = os.path.join(processed_knesset_data_path,"knessets","all_knessets_jsons.jsonl")
factions_jsonl_file_path = os.path.join(processed_knesset_data_path,"factions","factions_jsons.jsonl")

#PROTCOLS PATHS#
all_processed_protocols_path = os.path.join(processed_knesset_data_path, "protocols")
processed_committee_data_path = os.path.join(all_processed_protocols_path, "committee_protocols")
processed_plenary_data_path = os.path.join(all_processed_protocols_path, "plenary_protocols")
committees_processed_jsonl_protocols_files = os.path.join(processed_committee_data_path,"committee_protocols_jsons")
plenaries_processed_jsonl_protocols_files = os.path.join(processed_plenary_data_path,"plenary_protocols_jsons")
committees_processed_data_jsonl_file_path = os.path.join(processed_committee_data_path, "committees_protocols_jsonl.jsonl")
plenaries_processed_data_jsonl_file_path = os.path.join(processed_plenary_data_path,"plenaries_protocols_jsonl.jsonl")
conllu_files_path = os.path.join(processed_knesset_data_path,"protocols_conllu_files")
committee_protocols_conllu_files_path = os.path.join(conllu_files_path,"committee_protocols_conllu_files")
plenary_protocols_conllu_files_path = os.path.join(conllu_files_path,"plenary_protocols_conllu_files")

#SENTENCES PATHS#
sentences_jsonl_files = os.path.join(processed_knesset_data_path,"sentences_jsonl_files")
all_committee_full_sentences_jsonl_file = os.path.join(sentences_jsonl_files, "committee_full_sentences.jsonl")
all_plenary_full_sentences_jsonl_file = os.path.join(sentences_jsonl_files, "plenary_full_sentences.jsonl")
knesset_txt_files_path = os.path.join(processed_knesset_data_path, 'knesset_data_txt_files')
all_sentences_text_file = os.path.join(knesset_txt_files_path, 'all_sentences_text.txt')
committee_sentences_text_file = os.path.join(knesset_txt_files_path,"all_committee_sentences_text.txt")
plenary_sentences_text_file = os.path.join(knesset_txt_files_path,"all_plenary_sentences_text.txt")
knesset_csv_files_path = os.path.join(processed_knesset_data_path, "knesset_data_csv_files")
all_protocols_sentences_csv_file = os.path.join(knesset_csv_files_path,"all_protocols_sentences.csv")
plenary_processed_protocol_sentences_csv_file = os.path.join(knesset_csv_files_path, "plenary_processed_protocols_sentences.csv")
committee_processed_protocol_sentences_csv_file = os.path.join(knesset_csv_files_path, "committee_processed_protocols_sentences.csv")
sentences_lemmas_path = os.path.join(processed_knesset_data_path, "sentences_lemmas")
#PEOPLE PATHS#
all_people_path = os.path.join(processed_knesset_data_path,"people")
all_knesset_members_path = os.path.join(all_people_path,"all_knesset_members_jsons.jsonl")
people_jsons_by_protocol_path = os.path.join(all_people_path, "people_jsons_by_protocol")

#LOGS PATHS#
log_file_path = os.path.join(processed_knesset_data_path,"logs","log_file.txt")

###############CONSTANTS#############
EMPTY_STRING = ""
MONTHS = ['ינואר','פברואר','מרץ','מרס','מארס','אפריל','מאי','יוני','יולי','אוגוסט','ספטמבר','אוקטובר','נובמבר','דצמבר']
NUMBER_OF_KNESSESTS_IN_CORPUS = 24
FIRST_YEAR_IN_CORPUS = 1992
LAST_YEAR_IN_CORPUS = 2022
PROTOCOL_TYPES = ('committee', 'plenary')
SPLIT_SENTENCES_TOOL_OPTIONS = ['infoneto','udpipe']
FIRST_YEAR_MONTHS = ['01','02','03','04','05','06']
SECOND_YEAR_MONTHS = ['07','08','09','10','11','12']

WINTER_CHAIR_MONTHS = ["10", "11", "12", "01", "02", "03"]
WINTER_PERIOD_FIRST_YEAR_MONTHS = ["10", "11", "12"]
WINTER_PERIOD_SECOND_YEAR_MONTHS = ["01", "02", "03"]
SUMMER_PERIOD_MONTHS = ["04", "05", "06", "07", "08", "09"]



###DEFAULTS####
MAX_SPEAKER_NAME_LENGTH = 8
DATE_FORMAT = '%Y-%m-%d %H:%M'
SPLIT_SENTENCES_TOOL = SPLIT_SENTENCES_TOOL_OPTIONS[1]#0-'infoneto' , 1- 'udpipe'

