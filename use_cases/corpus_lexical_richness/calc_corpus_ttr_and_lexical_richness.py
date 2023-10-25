import re
from wordfreq import word_frequency, tokenize
from lexicalrichness import LexicalRichness
from Knesset_corpus_processing.corpus_statistic_functions import *
def calc_mean_word_rank_per_half_year_chair_equal_chunks(sentences_csv_file, n=1000, output_txt_file = 'mean_word_rank_per_half_year.txt', chair_seperation=False):
    per_half_year_and_chunk_sum_of_frequencies = {}
    per_half_year_and_chunk_total_num_of_tokens = {}
    chunk_counter = {}
    all_protocols_chunk_container = pd.read_csv(
        sentences_csv_file, chunksize=10000, parse_dates=['protocol_date'])
    for csv_chunk in all_protocols_chunk_container:
        for year_num in range(FIRST_YEAR_IN_CORPUS-1,LAST_YEAR_IN_CORPUS):#This algorithm adds 1 to year for summer months
            first_half_of_year_df = get_only_first_half_year_sentences(csv_chunk, year_num, chair_seperation=chair_seperation)
            second_half_of_year_df = get_only_second_half_year_sentences(csv_chunk, year_num, chair_seperation=chair_seperation)
            two_halfs_of_year = [first_half_of_year_df, second_half_of_year_df]
            for half_year_df, half_year_num in zip(two_halfs_of_year, range(len(two_halfs_of_year))):
                half_year_name, year = get_year_and_half_year_name(chair_seperation, half_year_num, year_num)
                calc_sum_of_word_frequency_for_each_chunk_in_half_year(chunk_counter, half_year_name, n,
                                                                           per_half_year_and_chunk_sum_of_frequencies,
                                                                           per_half_year_and_chunk_total_num_of_tokens,
                                                                           half_year_df, year)
    calc_avg_word_freq_of_all_chunks_per_half_year(chair_seperation, chunk_counter, n, output_txt_file,
                                                   per_half_year_and_chunk_sum_of_frequencies,
                                                   per_half_year_and_chunk_total_num_of_tokens, two_halfs_of_year)


def calc_avg_word_freq_of_all_chunks_per_half_year(chair_seperation, chunk_counter, n, output_txt_file,
                                                   per_half_year_and_chunk_sum_of_frequencies,
                                                   per_half_year_and_chunk_total_num_of_tokens, two_halfs_of_year):
    if os.path.exists(output_txt_file):
        os.remove(output_txt_file)
    per_half_year_mean_rank = {}
    for year_num in range(FIRST_YEAR_IN_CORPUS - 1, LAST_YEAR_IN_CORPUS):
        for half_year_num in range(len(two_halfs_of_year)):
            half_year_name, year = get_year_and_half_year_name(chair_seperation, half_year_num, year_num)
            per_half_year_sum_of_avg_chunk_freq = 0
            num_of_chunks_in_half_year = chunk_counter.get((year, half_year_name), 0)
            if num_of_chunks_in_half_year == 0:
                continue
            for chunk_num in range(num_of_chunks_in_half_year + 1):
                if per_half_year_and_chunk_total_num_of_tokens[(year, half_year_name, chunk_num)] < n:
                    continue
                else:
                    half_year_and_chunk_sum_of_frequencies = per_half_year_and_chunk_sum_of_frequencies[
                        (year, half_year_name, chunk_num)]
                    chunk_avg_freq = (half_year_and_chunk_sum_of_frequencies / n)
                    per_half_year_sum_of_avg_chunk_freq += chunk_avg_freq
            per_half_year_mean_rank[
                (year, half_year_name)] = per_half_year_sum_of_avg_chunk_freq / num_of_chunks_in_half_year

            with open(output_txt_file, 'a') as file:
                file.write(
                    f'in year {year} part {half_year_name} the {n} size chunk avg mean rank is {per_half_year_mean_rank[(year, half_year_name)]}.\n')
            print(
                f'in year {year} part {half_year_name} the {n} size chunk avg mean rank is {per_half_year_mean_rank[(year, half_year_name)]}.')


def calc_sum_of_word_frequency_for_each_chunk_in_half_year(chunk_counter, half_year_name, n,
                                                           per_half_year_and_chunk_sum_of_frequencies,
                                                           per_half_year_and_chunk_total_num_of_tokens, half_year_df, year):
    for index, row in half_year_df.iterrows():
        text = row["sentence_text"]
        tokens = tokenize(text, "he")
        num_of_tokens = len(tokens)
        chunk_num = chunk_counter.get((year, half_year_name), 0)
        half_year_chunk_total_num_of_tokens = per_half_year_and_chunk_total_num_of_tokens.get(
            (year, half_year_name, chunk_num), 0)
        if half_year_chunk_total_num_of_tokens + num_of_tokens <= n:
            half_year_chunk_total_num_of_tokens += num_of_tokens
            update_partial_chunk_word_freq_dicts_with_new_tokens(chunk_num, half_year_chunk_total_num_of_tokens,
                                                                 half_year_name,
                                                                 per_half_year_and_chunk_sum_of_frequencies,
                                                                 per_half_year_and_chunk_total_num_of_tokens, tokens,
                                                                 year)
        else:
            tokens_left_unil_n = n - half_year_chunk_total_num_of_tokens
            this_chunk_tokens = update_complete_chunk_word_freq_dicts(chunk_num, half_year_name, n,
                                                                      per_half_year_and_chunk_sum_of_frequencies,
                                                                      per_half_year_and_chunk_total_num_of_tokens,
                                                                      tokens, tokens_left_unil_n, year)
            chunk_num += 1
            chunk_counter[(year, half_year_name)] = chunk_num
            update_new_partial_chunk_word_freq_dicts(chunk_num, half_year_name,
                                                     per_half_year_and_chunk_sum_of_frequencies,
                                                     per_half_year_and_chunk_total_num_of_tokens, this_chunk_tokens,
                                                     tokens, year)


def update_new_partial_chunk_word_freq_dicts(chunk_num, half_year_name, per_half_year_and_chunk_sum_of_frequencies,
                                             per_half_year_and_chunk_total_num_of_tokens, this_chunk_tokens, tokens,
                                             year):
    new_chunk_tokens = [token for token in tokens if token not in this_chunk_tokens]
    half_year_and_chunk_sum_of_frequencies = per_half_year_and_chunk_sum_of_frequencies.get(
        (year, half_year_name, chunk_num), 0)
    for token in new_chunk_tokens:
        word_freq = word_frequency(token, "he")
        half_year_and_chunk_sum_of_frequencies += word_freq
    per_half_year_and_chunk_sum_of_frequencies[
        (year, half_year_name, chunk_num)] = half_year_and_chunk_sum_of_frequencies
    per_half_year_and_chunk_total_num_of_tokens[(year, half_year_name, chunk_num)] = len(new_chunk_tokens)


def update_complete_chunk_word_freq_dicts(chunk_num, half_year_name, n, per_half_year_and_chunk_sum_of_frequencies,
                                          per_half_year_and_chunk_total_num_of_tokens, tokens, tokens_left_unil_n,
                                          year):
    this_chunk_tokens = []
    for index in range(tokens_left_unil_n):
        this_chunk_tokens.append(tokens[index])
    half_year_and_chunk_sum_of_frequencies = per_half_year_and_chunk_sum_of_frequencies.get(
        (year, half_year_name, chunk_num), 0)
    for token in this_chunk_tokens:
        word_freq = word_frequency(token, "he")
        half_year_and_chunk_sum_of_frequencies += word_freq
    per_half_year_and_chunk_sum_of_frequencies[
        (year, half_year_name, chunk_num)] = half_year_and_chunk_sum_of_frequencies
    per_half_year_and_chunk_total_num_of_tokens[(year, half_year_name, chunk_num)] = n
    return this_chunk_tokens


def update_partial_chunk_word_freq_dicts_with_new_tokens(chunk_num, half_year_chunk_total_num_of_tokens, half_year_name,
                                                         per_half_year_and_chunk_sum_of_frequencies,
                                                         per_half_year_and_chunk_total_num_of_tokens, tokens, year):
    per_half_year_and_chunk_total_num_of_tokens[
        (year, half_year_name, chunk_num)] = half_year_chunk_total_num_of_tokens
    half_year_and_chunk_sum_of_frequencies = per_half_year_and_chunk_sum_of_frequencies.get(
        (year, half_year_name, chunk_num), 0)
    for token in tokens:
        word_freq = word_frequency(token, "he")
        half_year_and_chunk_sum_of_frequencies += word_freq
    per_half_year_and_chunk_sum_of_frequencies[
        (year, half_year_name, chunk_num)] = half_year_and_chunk_sum_of_frequencies


def calc_ttr_lexical_richness_per_half_year_chair_equal_chunks(sentences_csv_file, n=1000, output_txt_file = 'avg_ttr_per_half_year.txt', chair_seperation=False):
    per_half_year_and_chunk_ttr = {}
    half_year_and_partial_chunk_tokens = {}
    per_half_year_and_chunk_total_num_of_tokens = {}
    chunk_counter = {}
    all_protocols_chunk_container = pd.read_csv(
        sentences_csv_file, chunksize=10000, parse_dates=['protocol_date'])
    for csv_chunk in all_protocols_chunk_container:
        for year_num in range(FIRST_YEAR_IN_CORPUS-1,LAST_YEAR_IN_CORPUS):#this algorithm add 1 to year for summer months
            first_half_of_year_df = get_only_first_half_year_sentences(csv_chunk, year_num, chair_seperation=chair_seperation)
            second_half_of_year_df = get_only_second_half_year_sentences(csv_chunk, year_num, chair_seperation=chair_seperation)
            two_halfs_of_year = [first_half_of_year_df, second_half_of_year_df]
            for half_year_df, half_year_num in zip(two_halfs_of_year, range(len(two_halfs_of_year))):
                half_year_name, year = get_year_and_half_year_name(chair_seperation,half_year_num, year_num )
                calc_ttr_for_each_half_year_chunk(chunk_counter, half_year_and_partial_chunk_tokens, half_year_df,
                                                  half_year_name, n, per_half_year_and_chunk_total_num_of_tokens,
                                                  per_half_year_and_chunk_ttr, year)

    calc_avg_ttr_of_all_chunks_per_half_year(chair_seperation, chunk_counter, n, output_txt_file,
                                             per_half_year_and_chunk_total_num_of_tokens, per_half_year_and_chunk_ttr,
                                             two_halfs_of_year)


def calc_ttr_for_each_half_year_chunk(chunk_counter, half_year_and_partial_chunk_tokens, half_year_df, half_year_name,
                                      n, per_half_year_and_chunk_total_num_of_tokens, per_half_year_and_chunk_ttr,
                                      year):
    for index, row in half_year_df.iterrows():
        text = row["sentence_text"]
        text = re.sub(r'[^\w]', ' ', text)
        tokens = text.split()
        num_of_tokens = len(tokens)
        chunk_num = chunk_counter.get((year, half_year_name), 0)
        year_chunk_total_num_of_tokens = per_half_year_and_chunk_total_num_of_tokens.get(
            (year, half_year_name, chunk_num), 0)
        if year_chunk_total_num_of_tokens + num_of_tokens < n:
            year_chunk_total_num_of_tokens += num_of_tokens
            update_partial_chunks_with_sentence_tokens(chunk_num, half_year_and_partial_chunk_tokens, half_year_name,
                                                       per_half_year_and_chunk_total_num_of_tokens, tokens, year,
                                                       year_chunk_total_num_of_tokens)
        else:
            tokens_left_unil_n = n - year_chunk_total_num_of_tokens
            this_chunk_tokens = []
            for index in range(tokens_left_unil_n):
                this_chunk_tokens.append(tokens[index])
            update_complete_chunk_dicts_with_new_tokens(chunk_num, half_year_and_partial_chunk_tokens,half_year_name, n,
                                                                                             per_half_year_and_chunk_total_num_of_tokens,
                                                                                             this_chunk_tokens, year)
            chunk_ttr = calc_chunk_ttr(chunk_num, half_year_and_partial_chunk_tokens, half_year_name, year)
            per_half_year_and_chunk_ttr[(year, half_year_name, chunk_num)] = chunk_ttr
            half_year_and_partial_chunk_tokens[(year, half_year_name,
                                                chunk_num)] = []  # once we calculated measures on chunk we don't need it anymore. for memory reasons
            chunk_num += 1
            chunk_counter[(year, half_year_name)] = chunk_num
            update_new_partial_chunk_dicts_with_rest_of_tokens(chunk_num, half_year_and_partial_chunk_tokens,
                                                               half_year_name,
                                                               per_half_year_and_chunk_total_num_of_tokens,
                                                               this_chunk_tokens, tokens, year)


def update_new_partial_chunk_dicts_with_rest_of_tokens(chunk_num, half_year_and_partial_chunk_tokens, half_year_name,
                                                       per_half_year_and_chunk_total_num_of_tokens, this_chunk_tokens,
                                                       tokens, year):
    new_chunk_tokens = [token for token in tokens if token not in this_chunk_tokens]
    half_year_and_partial_chunk_tokens[(year, half_year_name, chunk_num)] = new_chunk_tokens
    per_half_year_and_chunk_total_num_of_tokens[(year, half_year_name, chunk_num)] = len(new_chunk_tokens)


def calc_chunk_ttr(chunk_num, half_year_and_partial_chunk_tokens, half_year_name, year):
    this_year_and_partial_chunk_tokens = half_year_and_partial_chunk_tokens[(year, half_year_name, chunk_num)]
    chunk_text = ' '.join(this_year_and_partial_chunk_tokens)
    lex = LexicalRichness(chunk_text)
    chunk_ttr = lex.ttr
    return chunk_ttr


def update_complete_chunk_dicts_with_new_tokens(chunk_num, half_year_and_partial_chunk_tokens, half_year_name, n,
                                                per_half_year_and_chunk_total_num_of_tokens, this_chunk_tokens, year):
    this_year_and_partial_chunk_tokens = half_year_and_partial_chunk_tokens.get(
        (year, half_year_name, chunk_num), list())
    this_year_and_partial_chunk_tokens.extend(this_chunk_tokens)
    half_year_and_partial_chunk_tokens[(year, half_year_name, chunk_num)] = this_year_and_partial_chunk_tokens
    per_half_year_and_chunk_total_num_of_tokens[(year, half_year_name, chunk_num)] = n


def update_partial_chunks_with_sentence_tokens(chunk_num, half_year_and_partial_chunk_tokens, half_year_name,
                                               per_half_year_and_chunk_total_num_of_tokens, tokens, year,
                                               year_chunk_total_num_of_tokens):
    per_half_year_and_chunk_total_num_of_tokens[
        (year, half_year_name, chunk_num)] = year_chunk_total_num_of_tokens
    this_year_and_partial_chunk_tokens = half_year_and_partial_chunk_tokens.get(
        (year, half_year_name, chunk_num), list())
    this_year_and_partial_chunk_tokens.extend(tokens)
    half_year_and_partial_chunk_tokens[(year, half_year_name, chunk_num)] = this_year_and_partial_chunk_tokens


def calc_avg_ttr_of_all_chunks_per_half_year(chair_seperation, chunk_counter, n, output_txt_file,
                                             per_half_year_and_chunk_total_num_of_tokens, per_half_year_and_chunk_ttr):
    num_of_halfs = 2
    if os.path.exists(output_txt_file):
        os.remove(output_txt_file)
    per_half_year_avg_ttr = {}
    for year_num in range(FIRST_YEAR_IN_CORPUS - 1, LAST_YEAR_IN_CORPUS):
        for half_year_num in range(num_of_halfs):
            half_year_name, year = get_year_and_half_year_name(chair_seperation, half_year_num, year_num)
            per_half_year_chunk_ttr_sum = 0
            num_of_chunks_in_half_year = chunk_counter.get((year, half_year_name), 0)
            if num_of_chunks_in_half_year == 0:
                continue
            for chunk_num in range(num_of_chunks_in_half_year + 1):
                if per_half_year_and_chunk_total_num_of_tokens[(year, half_year_name, chunk_num)] < n:
                    continue
                else:
                    per_half_year_chunk_ttr_sum += per_half_year_and_chunk_ttr[(year, half_year_name, chunk_num)]
            per_half_year_avg_ttr[(year, half_year_name)] = per_half_year_chunk_ttr_sum / num_of_chunks_in_half_year
            with open(output_txt_file, 'a') as file:
                file.write(
                    f'in year {year} in part {half_year_name} the {n} size chunk avg ttr is {per_half_year_avg_ttr[(year, half_year_name)]} .\n')
            print(
                f'in year {year} in part {half_year_name} the {n} size chunk avg ttr is {per_half_year_avg_ttr[(year, half_year_name)]} .')








if __name__ == '__main__':

    plenary_sentences_path = plenary_processed_protocol_sentences_csv_file
    committee_sentences_path = committee_processed_protocol_sentences_csv_file

    calc_ttr_lexical_richness_per_half_year_chair_equal_chunks(plenary_sentences_path, output_txt_file="plenary_avg_ttr_per_chair.txt", chair_seperation=True)
    plenary_per_half_year_ttr = create_half_year_score_dict_from_file("plenary_avg_ttr_per_chair.txt")
    plot_per_period_values(plenary_per_half_year_ttr, half_year=True, title="plenary_avg_ttr_per_chair")
    data = list(plenary_per_half_year_ttr.values())
    apply_Mann_Kendall_Test(data, significance_level=0.05, data_name="plenary_per_chair_ttr")

    calc_mean_word_rank_per_half_year_chair_equal_chunks(plenary_sentences_path, n=1000,
                                                         output_txt_file='plenary_mean_word_rank_per_chair.txt',
                                                         chair_seperation=True)
    plenary_per_half_year_mean_word_rank = create_half_year_score_dict_from_file("plenary_mean_word_rank_per_chair.txt")
    plot_per_period_values(plenary_per_half_year_mean_word_rank, half_year=True, title="plenary_mean_word_rank_per_chair")
    data = list(plenary_per_half_year_mean_word_rank.values())
    apply_Mann_Kendall_Test(data, significance_level=0.05, data_name="plenary_per_chair_mean_word_frequency")

    calc_ttr_lexical_richness_per_half_year_chair_equal_chunks(committee_sentences_path,
                                                               output_txt_file="committee_avg_ttr_per_chair.txt",
                                                               chair_seperation=True)
    committee_per_half_year_ttr = create_half_year_score_dict_from_file("committee_avg_ttr_per_chair.txt")
    plot_per_period_values(committee_per_half_year_ttr, half_year=True, title="committee_avg_ttr_per_chair")
    data = list(committee_per_half_year_ttr.values())
    apply_Mann_Kendall_Test(data, significance_level=0.05, data_name="committee_per_chair_ttr")

    calc_mean_word_rank_per_half_year_chair_equal_chunks(committee_sentences_path, n=1000,output_txt_file='committee_mean_word_rank_per_chair.txt', chair_seperation=True)
    committee_per_half_year_mean_word_rank = create_half_year_score_dict_from_file(
        'committee_mean_word_rank_per_chair.txt')
    plot_per_period_values(committee_per_half_year_mean_word_rank, half_year=True,
                           title="committee_mean_word_rank_per_chair")
    data = list(committee_per_half_year_mean_word_rank.values())
    apply_Mann_Kendall_Test(data, significance_level=0.05, data_name="committee_per_chair_mean_word_frequency")
