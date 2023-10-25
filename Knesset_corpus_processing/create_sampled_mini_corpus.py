import random
import shutil

from params_config import *

all_protocols_path = all_doc_protocols_path
committees_path = committees_protocols_doc_files_path
plenaries_path = plenaries_protocols_doc_files_path
mini_corpus_committees_path =os.path.join(data_path,'mini_knesset','Committees')
mini_corpus_plenaries_path = os.path.join(data_path,'mini_knesset','plenaries')



def sample_files_for_mini_corpus(protocold_to_copy_path, protocol_type):
    for knesset_dir in os.listdir(protocold_to_copy_path):
        knesset_dir_path = os.path.join(protocold_to_copy_path, knesset_dir, 'word_files')
        list_of_knesset_files = os.listdir(knesset_dir_path)
        chosen_knesset_files = random.sample(list_of_knesset_files, int(0.01 * len(list_of_knesset_files)+1))
        if protocol_type == 'committee':
            mini_corpus_protocols_path = mini_corpus_committees_path
        else:
            mini_corpus_protocols_path = mini_corpus_plenaries_path
        mini_corpus_knesset_dir_path = os.path.join(mini_corpus_protocols_path, knesset_dir)
        mini_corpus_knesset_word_files_path = os.path.join(mini_corpus_knesset_dir_path, 'word_files')
        if not os.path.exists(mini_corpus_knesset_dir_path):
            os.mkdir(mini_corpus_knesset_dir_path)
        if not os.path.exists(mini_corpus_knesset_word_files_path):
            os.mkdir(mini_corpus_knesset_word_files_path)
        for file_name in chosen_knesset_files:
            file_path = os.path.join(knesset_dir_path, file_name)
            shutil.copyfile(file_path, os.path.join(mini_corpus_knesset_word_files_path, file_name))


sample_files_for_mini_corpus(committees_path, 'committee')
sample_files_for_mini_corpus(plenaries_path, 'plenary')

