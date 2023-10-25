import os.path
import statistics
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
import pymannkendall as mk
from params_config import *
from statsmodels.stats.proportion import proportions_ztest



def calc_number_of_tokens_in_corpus(path_to_sentences_text_file):
    total_num_of_tokens = 0
    unique_tokens = set()
    with open(path_to_sentences_text_file, encoding="utf-8") as file:
        for line in file:
            sent_tokens = line.split()
            num_of_tokens_in_sent = len(sent_tokens)
            unique_tokens.update(sent_tokens)
            total_num_of_tokens += num_of_tokens_in_sent
    print(f"total number of tokens in corpus: {total_num_of_tokens}")
    print(f"number of unique tokens: {len(unique_tokens)}")

def calc_number_of_sentences_in_corpus(path_to_sentences_text_file):
    total_num_of_sentences = 0
    with open(path_to_sentences_text_file, encoding="utf-8") as file:
        for line in file:
            total_num_of_sentences +=1

    print(f"total number of sentences in corpus: {total_num_of_sentences}")
def calc_all_sentences_length(path):
    all_sents_length = []
    with open(path, encoding="utf-8") as file:
        for line in file:
            sent_length = len(line.split())
            all_sents_length.append(sent_length)
    return all_sents_length

def calc_avg_length_of_knesset_sentences():
    all_sents_length = calc_all_sentences_length(os.path.join(knesset_txt_files_path, 'all_sentences_text.txt'))
    mean = statistics.mean(all_sents_length)
    std = statistics.stdev(all_sents_length)
    median = statistics.median(all_sents_length)
    print(f'The average sentence length is: {mean}')
    print(f'The standard deviation is {std} ')
    print(f'The median is {median} ')

    return all_sents_length

def calc_histogram(lengths_list):
    bins = {}
    for length in lengths_list:
        if length<=4:
            res = bins.get("01-04", 0)
            res += 1
            bins["01-04"] = res
        elif length<= 10:
            res = bins.get("05-10", 0)
            res +=1
            bins["05-10"] = res
        elif length<= 20:
            res = bins.get("11-20", 0)
            res +=1
            bins["11-20"] = res
        elif length <=30:
            res = bins.get("21-30", 0)
            res += 1
            bins["21-30"] = res
        elif length <=40:
            res = bins.get("31-40", 0)
            res += 1
            bins["31-40"] = res
        else:
            res = bins.get("41+", 0)
            res += 1
            bins["41+"] = res

    myKeys = list(bins.keys())
    myKeys.sort()
    sorted_bins = {i: bins[i] for i in myKeys}
    print(sorted_bins)
    return sorted_bins


def plot_histogram(bins_dict, title="histogram", rotation='horizontal', fig_size_0=None, fig_size_1 = None, ylim_min=-1, ylim_max=-1, y_ticks_min=-1, y_ticks_max=-1, y_ticks_step=-1):
    if fig_size_0 and fig_size_1:
        plt.figure(figsize=(fig_size_0, fig_size_1))
    plt.bar(bins_dict.keys(), bins_dict.values(), 1, color='deeppink', edgecolor=(0, 0, 0))
    if ylim_min>=0 and ylim_max>=0:
        plt.ylim(ylim_min, ylim_max)
    for i, v in enumerate(bins_dict.values()):
        plt.text(i - 0.45, v + 0.01, str(v), color='black', fontweight='bold', fontsize=12)
    if y_ticks_min>=0 and y_ticks_max>=0 and y_ticks_step>-0:
        plt.yticks(np.arange(y_ticks_min, y_ticks_max, y_ticks_step),fontsize=10)
    plt.xticks(rotation=rotation)
    plt.title(title)
    plt.show()


def get_only_first_half_year_sentences(csv_chunk, year, chair_seperation=False):
    if chair_seperation:
        start_year_df = csv_chunk[csv_chunk['protocol_date'].dt.strftime('%Y')==str(year)]
        end_year_df = csv_chunk[csv_chunk['protocol_date'].dt.strftime('%Y')==str(year+1)]
        first_half_of_start_year_df = start_year_df[start_year_df['protocol_date'].dt.strftime('%m').isin(WINTER_PERIOD_FIRST_YEAR_MONTHS)]
        first_half_of_end_year_df = end_year_df[end_year_df['protocol_date'].dt.strftime('%m').isin(WINTER_PERIOD_SECOND_YEAR_MONTHS)]
        first_half_of_year_df = pd.concat([first_half_of_start_year_df, first_half_of_end_year_df], ignore_index=True)
    else:
        year_df = csv_chunk[csv_chunk['protocol_date'].dt.strftime('%Y')==str(year)]
        first_half_of_year_df = year_df[year_df['protocol_date'].dt.strftime('%m').isin(FIRST_YEAR_MONTHS)]
    return first_half_of_year_df

def get_only_second_half_year_sentences(csv_chunk, year, chair_seperation=False):
    if chair_seperation:
        year_df = csv_chunk[csv_chunk['protocol_date'].dt.strftime('%Y')==str(year+1)]
        second_half_of_year_df = year_df[year_df['protocol_date'].dt.strftime('%m').isin(SUMMER_PERIOD_MONTHS)]
    else:
        year_df = csv_chunk[csv_chunk['protocol_date'].dt.strftime('%Y')==str(year)]
        second_half_of_year_df = year_df[year_df['protocol_date'].dt.strftime('%m').isin(SECOND_YEAR_MONTHS)]
    return second_half_of_year_df


def get_year_and_half_year_name(chair_seperation, half_year_num, year_num):
    if chair_seperation:
        if half_year_num == 0:
            half_year_name = "winter"
            year = year_num
        elif half_year_num == 1:
            half_year_name = "summer"
            year = year_num + 1
        else:
            print("what went wrong?")
    else:
        half_year_name = half_year_num
        year = year_num
    return half_year_name, year





def how_many_protocols_per_year(path_to_protocols, protocol_type="committee"):
    knesset_nums = os.listdir(path_to_protocols)
    for knesset_num in knesset_nums:
        word_year_dirs_path = os.path.join(path_to_protocols,knesset_num,"word_files")
        years_dirs = os.listdir(word_year_dirs_path)
        for year in years_dirs:
            year_protocols_path = os.path.join(word_year_dirs_path, year)
            num_of_protocols_in_year = len(os.listdir(year_protocols_path))
            print(f'The number of {protocol_type} protocols in knesset {knesset_num} in year {year} is: {num_of_protocols_in_year}')



def apply_Mann_Kendall_Test(data, significance_level=0.05, data_name=""):
    res = mk.original_test(data)
    print(f'p_value is: {res.p}')
    if res.p < significance_level:
        print(
            f'the p-value of the test is lower than significance level = {significance_level} so there is statistically significant evidence that a trend is present in the time series data: {data_name}')
        print(f'the trend is: {res.trend}\n')
    else:
        print(f'the p-value of the test is higher than significance level in the time series data: {data_name}\n')

def calculate_number_of_sentences_in_sentences_jsonl_file(jsonl_file):
    num_of_sentences = 0
    with open(jsonl_file,encoding="utf-8") as file:
        for line in file:
            num_of_sentences +=1
    print(f' num of sentences in {os.path.basename(jsonl_file)} is : {num_of_sentences}')
    return num_of_sentences

def calc_number_of_gender_sentences_in_huge_jsonl_file(full_sentences_jsonl, gender):
    num_of_sentences = 0
    with open(full_sentences_jsonl, encoding="utf-8") as file:
        for line in file:
            if f'"speaker_gender": "{gender}"' in line:
                num_of_sentences += 1
    print(f'number of {gender} sentences in {os.path.basename(full_sentences_jsonl)} is : {num_of_sentences}')
    return num_of_sentences



if __name__ == '__main__':
    calc_number_of_tokens_in_corpus(committee_sentences_text_file)
    calc_avg_length_of_knesset_sentences()
    all_sentences_lengths = calc_all_sentences_length(all_sentences_text_file)
    bins = calc_histogram(all_sentences_lengths)
    plot_histogram(bins)

    all_committee_sentences_lengths = calc_all_sentences_length(committee_sentences_text_file)
    bins = calc_histogram(all_committee_sentences_lengths)
    plot_histogram(bins, title="committee sentences histogram")

    all_plenary_sentences_lengths = calc_all_sentences_length(plenary_sentences_text_file)
    bins = calc_histogram(all_plenary_sentences_lengths)
    plot_histogram(bins, title="plenary sentences histogram")

    calc_number_of_tokens_in_corpus(all_sentences_text_file)

    calc_number_of_sentences_in_corpus(all_sentences_text_file)
    calc_number_of_sentences_in_corpus(committee_sentences_text_file)
    calc_number_of_sentences_in_corpus(plenary_sentences_text_file)

    calc_number_of_gender_sentences_in_huge_jsonl_file(all_plenary_full_sentences_jsonl_file, gender="female" )
    calc_number_of_gender_sentences_in_huge_jsonl_file(all_plenary_full_sentences_jsonl_file, gender="male" )
    calc_number_of_gender_sentences_in_huge_jsonl_file(all_committee_full_sentences_jsonl_file, gender="male" )
    calc_number_of_gender_sentences_in_huge_jsonl_file(all_committee_full_sentences_jsonl_file,gender="female" )



