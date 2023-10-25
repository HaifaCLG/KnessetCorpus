import time
import requests
token = '#####'



def split_to_sentences_request(text):
    request = {'token': token, 'text': text}
    num_of_tries = 5
    for i in range(num_of_tries):
        try:
            result = requests.post('https://hebrew-nlp.co.il/service/preprocess/sentencer', json=request).json()
        except Exception as e:
            if i == num_of_tries-1:
                print(f'Error: could not split sentences')
                result = []
            print(f'Error_{i}: split sentences request failed with exception: {e}')
            time.sleep(5)

    return result


