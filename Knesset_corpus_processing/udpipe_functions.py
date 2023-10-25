import logging
import time
from subprocess import Popen, PIPE
from params_config import *
import requests

logging.basicConfig(filename=log_file_path, level=logging.INFO,  format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S', encoding='utf-8')

LINUX = False

def udpipe_split_sentences_cmd(text):
    if LINUX:
        cmd = "./udpipe", "--tokenize", "iahlt-1.udpipe"
    else:
        cmd = ["wsl","~/workspace/udpipe/src/udpipe", "--tokenize", "~/workspace/udpipe/src/iahlt-1.udpipe"]
    proc = Popen(cmd,shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE, text=True, encoding='utf-8')
    output, _ = proc.communicate(input=text)
    sentences_and_tokens = output.split('text =')
    sentences = [sent_data.split('\n')[0].strip() for sent_data in sentences_and_tokens]
    sentences.remove(sentences[0])
    return sentences

def udpipe_split_sentences_rest(text):
    url = f'http://localhost:8080/process?tokenizer=iahlt-1&data={text}'

    payload = {}
    headers = {}

    try:
        response = requests.request("POST", url, headers=headers, data=payload)
    except:
        time.sleep(0.2)
        try:
            response = requests.request("POST", url, headers=headers, data=payload)
        except Exception as e:
            logging.info(f'inside udpipe_func: exception was: {e} ')
            return []
    try:
        output = response.json().get("result")
    except Exception as e:
        logging.info(f'inside udpipe_func: exception was: {e} ')
        return []
    sentences_and_tokens = output.split('text =')
    sentences = [sent_data.split('\n')[0].strip() for sent_data in sentences_and_tokens]
    sentences.remove(sentences[0])
    return sentences

