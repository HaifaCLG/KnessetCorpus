from Knesset_corpus_processing.corpus_statistic_functions import *


def create_year_score_dict_from_file(path):
    per_year_ttr = {}
    with open(path) as f:
        for line in f:
           year = line.split("in year ")[1].split()[0]
           score = float(line.split(" is ")[1].split()[0][:-1])
           per_year_ttr[year] = score
    return per_year_ttr

def create_half_year_score_dict_from_file(path):
    per_year_ttr = {}
    with open(path) as f:
        for line in f:
           year = line.split("in year ")[1].split()[0]
           part = line.split("part ")[1].split()[0]
           score = float(line.split(" is ")[1].split()[0][:-1])
           per_year_ttr[(year,part)] = score
    return per_year_ttr

def plot_per_period_values(per_year_dict, title ="ttr per year", half_year=False, ylim_min=None, ylim_max=None, y_ticks_min=None, y_ticks_max=None, y_ticks_step=None, text_up=0.001, after_digit="%.3f"):
    years = list(per_year_dict.keys())
    values = list(per_year_dict.values())
    if half_year:
        years = [f'{years[i][0]}-{years[i][1]}' for i in range(len(years))]
    plt.figure(figsize=(100, 50))
    if ylim_min and ylim_max:
        plt.ylim(ylim_min, ylim_max)
    if y_ticks_min and y_ticks_max and y_ticks_step:
        plt.yticks(np.arange(y_ticks_min, y_ticks_max, y_ticks_step),fontsize=60)
    else:
        plt.yticks(fontsize=60)
    plt.xticks(fontsize=50, rotation='vertical')


    plt.bar(range(len(per_year_dict)), values, width=0.8, tick_label=years, color='deeppink', edgecolor=(0, 0, 0))
    plt.title(title, fontsize = 80)
    for i, v in enumerate(values):
        plt.text(i - 0.45, v + text_up, str(after_digit % v),color = 'black', fontweight = 'bold',fontsize=45)

    plt.show()
    print("done!")