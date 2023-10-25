from elasticsearch import Elasticsearch

elastic_ip = "####"

es_username = "####"
es_password = "####"
es = Elasticsearch(f'http://{elastic_ip}',http_auth=(es_username, es_password), timeout=100)