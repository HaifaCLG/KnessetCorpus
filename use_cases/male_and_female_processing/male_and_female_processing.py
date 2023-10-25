import numpy as np
from statsmodels.stats.proportion import proportions_ztest


from Knesset_corpus_processing.aux_functions import *
from Knesset_corpus_processing.log_odds_functions import *
from use_cases.male_and_female_processing.male_female_dataset_creation import \
    create_only_male_and_female_lemmas_list_from_huge_jsonl_file, \
    create_only_male_and_female_full_sentences_jsonl_from_huge_jsonl_file


def calculate_passive_and_active_voice_frequencies_for_each_gender(full_sentences_jsonl_files, gender):
    k = 20
    num_of_verbs = 0
    voice_dict = {}
    act_words_count_dict = {}
    pass_words_count_dict = {}
    for full_sentences_jsonl in full_sentences_jsonl_files:
        with open(full_sentences_jsonl, encoding="utf-8") as file:
            for line in file:
                if f'"speaker_gender": "{gender}"' in line:
                    try:
                        sent_entity = json.loads(line)
                    except Exception as e:
                        print(f'couldnt load json. error was: {e}')
                        continue
                    morph_fields = sent_entity['morphological_fields']
                    if morph_fields:
                        for token in morph_fields:
                            if token["upos"] == "VERB" or token["xpos"]=="VERB":
                                num_of_verbs +=1
                                if token["feats"]:
                                    voice = token["feats"].get("Voice","")
                                    if voice:
                                        num_of_voice_type = voice_dict.get(voice, 0)
                                        num_of_voice_type +=1
                                        voice_dict[voice] = num_of_voice_type
                                        if voice == "Act":
                                            verb = token["form"].strip()
                                            count = act_words_count_dict.get(verb, 0)
                                            count +=1
                                            act_words_count_dict[verb] = count
                                        elif voice == "Pass":
                                            verb = token["form"].strip()
                                            count = pass_words_count_dict.get(verb, 0)
                                            count += 1
                                            pass_words_count_dict[verb] = count
    print(f'in {gender} class- total number of verb tokens is {num_of_verbs}')
    for voice, num_of_voice in voice_dict.items():
        print(f'in {gender} class- number of {voice} tokens is: {num_of_voice}')
        voice_freq = (num_of_voice/num_of_verbs)*100
        print(f'in {gender} class- {voice} frequency: {voice_freq:.2f}%')
    sorted_act_words_count_dict = sorted(act_words_count_dict.items(), key=lambda x: x[1], reverse=True)
    k_most_freq_act_words = take(k, sorted_act_words_count_dict)
    sorted_pass_words_count_dict = sorted(pass_words_count_dict.items(), key=lambda x: x[1], reverse=True)
    k_most_freq_pass_words = take(k, sorted_pass_words_count_dict)

    print(f'{k} most frequent Act words in {gender} class: {k_most_freq_act_words}')
    print(f'{k} most frequent pass words in {gender} class: {k_most_freq_pass_words}')


def get_class_lemmas_list(dir_path, class_file_names):
    class_lemmas =[]
    for file in class_file_names:
        path = os.path.join(dir_path, file)
        file_lemmas = []
        with open(path, encoding="utf-8") as lemma_file:
            for line in lemma_file:
                if line.strip():
                    file_lemmas.append(line.strip())
        class_lemmas.extend(file_lemmas)
    return class_lemmas
def calc_by_gender_log_odds_with_lemmas(dir_path):
    LOAD_LEMMAS_FROM_FILE = True
    files = os.listdir(dir_path)
    female_file_names = []
    male_file_names = []
    for file in files:
        if "_female_" in file:
            female_file_names.append(file)
        elif "_male_" in file:
            male_file_names.append(file)
        else:
            print("not female or male file")
    if not LOAD_LEMMAS_FROM_FILE:
        female_lemmas = get_class_lemmas_list(dir_path, female_file_names)
        save_object(female_lemmas,"all_female_lemmas")
        male_lemmas = get_class_lemmas_list(dir_path, male_file_names)
        save_object(male_lemmas, "all_male_lemmas")
    else:
        female_lemmas = load_object("all_female_lemmas")
        male_lemmas = load_object("all_male_lemmas")
    print("finished loading lemmas")
    calc_log_odds_on_word_list(male_lemmas, female_lemmas, 50, class_a_name="class_male",
                                   class_b_name="class_female")


def check_proportion_significance_test():
    count = np.array([5747960,24331402])
    nobs = np.array([7624755,32471857])
    stat, pval = proportions_ztest(count,nobs)
    print('{0:0.3f}'.format(pval))
    print(pval)

if __name__ == '__main__':
    create_only_male_and_female_lemmas_list_from_huge_jsonl_file(all_plenary_full_sentences_jsonl_file, os.path.join(sentences_lemmas_path,"plenary_female_lemmas.txt"), gender="female")
    create_only_male_and_female_lemmas_list_from_huge_jsonl_file(all_plenary_full_sentences_jsonl_file, os.path.join(sentences_lemmas_path,"plenary_male_lemmas.txt"), gender="male")
    create_only_male_and_female_lemmas_list_from_huge_jsonl_file(all_committee_full_sentences_jsonl_file, os.path.join(sentences_lemmas_path,"committee_female_lemmas.txt"), gender="female")
    create_only_male_and_female_lemmas_list_from_huge_jsonl_file(all_committee_full_sentences_jsonl_file, os.path.join(sentences_lemmas_path,"committee_male_lemmas.txt"), gender="male")

    create_only_male_and_female_full_sentences_jsonl_from_huge_jsonl_file(all_plenary_full_sentences_jsonl_file, os.path.join(sentences_jsonl_files, "plenary_female_sentences.jsonl"), gender="female")
    create_only_male_and_female_full_sentences_jsonl_from_huge_jsonl_file(all_plenary_full_sentences_jsonl_file, os.path.join(sentences_jsonl_files, "plenary_male_sentences.jsonl"),gender="male")
    create_only_male_and_female_full_sentences_jsonl_from_huge_jsonl_file(all_committee_full_sentences_jsonl_file, os.path.join(sentences_jsonl_files, "committee_female_sentences.jsonl"), gender="female")
    create_only_male_and_female_full_sentences_jsonl_from_huge_jsonl_file(all_committee_full_sentences_jsonl_file, os.path.join(sentences_jsonl_files, "committee_male_sentences.jsonl"), gender="male")

    calculate_passive_and_active_voice_frequencies_for_each_gender([all_plenary_full_sentences_jsonl_file, all_committee_full_sentences_jsonl_file], gender="female")
    calculate_passive_and_active_voice_frequencies_for_each_gender([all_plenary_full_sentences_jsonl_file, all_committee_full_sentences_jsonl_file], gender="male")
    check_proportion_significance_test()

    calc_by_gender_log_odds_with_lemmas(sentences_lemmas_path)
