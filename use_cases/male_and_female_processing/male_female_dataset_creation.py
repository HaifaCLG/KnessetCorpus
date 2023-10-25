def create_only_male_and_female_full_sentences_jsonl_from_huge_jsonl_file(full_sentences_jsonl, output, gender):
    text_to_write = ""
    counter = 0
    max_lines_to_write = 1000
    with open(full_sentences_jsonl, encoding="utf-8") as file:
        for line in file:
            if f'"speaker_gender": "{gender}"' in line:
                counter +=1
                text_to_write += f'{line}'
                if counter < max_lines_to_write:
                    continue
                else:
                    with open(output,"a", encoding="utf-8") as output_file:
                        output_file.write(text_to_write)
                    text_to_write = ""
    with open(output, "a", encoding="utf-8") as output_file:
        output_file.write(text_to_write)


def create_only_male_and_female_lemmas_list_from_huge_jsonl_file(full_sentences_jsonl, output, gender):
    counter = 0
    max_lines_to_write = 1000
    lemmas = []
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
                        if token['lemma'] and token['lemma']!='–' and token['lemma']!= '_':
                            if token["upos"] != 'PUNCT':
                                lemmas.append(token['lemma'].strip())
                                counter +=1
                                if counter >= max_lines_to_write:
                                    text = "\n".join(lemmas)
                                    with open(output,"a", encoding="utf-8") as output_file:
                                        output_file.write(f'{text}\n')
                                    lemmas = []
                                    counter = 0

    text = "\n".join(lemmas)
    with open(output, "a", encoding="utf-8") as output_file:
        output_file.write(text)
