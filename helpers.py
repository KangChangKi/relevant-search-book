import requests
import json
import os
import pickle
import os.path
from pprint import pprint
from time import sleep

def flatten(l):
    [item for sublist in l for item in sublist]

def simplerExplain(explainJson, depth=0):
    result = " " * (depth * 2) + "%s, %s\n" % (explainJson['value'], explainJson['description'])
    #print json.dumps(explainJson, indent=True)
    if 'details' in explainJson:
        for detail in explainJson['details']:
            result += simplerExplain(detail, depth=depth+1)
    return result

def delete_index(index_name):
    resp = requests.delete(f"http://localhost:9200/{index_name}")
    print(resp.status_code)
    
def create_index(index_name, settings):
    headers = {"Content-Type": "application/json"}
    resp = requests.put(f"http://localhost:9200/{index_name}", headers=headers, data=json.dumps(settings))
    print(resp.status_code)
    print(resp.text)

def fill_index_bulk(bulkData):
    headers = {"Content-Type": "application/json"}
    resp = requests.post("http://localhost:9200/_bulk", headers=headers, data=bulkData)
    print(resp.status_code)

# usersSearch = 'basketball with cartoon aliens'
# query = {
#     'query': {
#         'multi_match': { 
#             'query': usersSearch,  #User's query
#             'fields': ['title^10', 'overview'],
#         }
#     },
#     'size': '100',
#     'explain': True
# }

def search(query, _print=True):
    headers = {"Content-Type": "application/json"}
    httpResp = requests.get('http://localhost:9200/tmdb/_search', headers=headers, data=json.dumps(query))
    
    if _print:
        searchHits = json.loads(httpResp.text)['hits']
        print("Num\tRelevance Score\t\tMovie Title\t\tOverview")
        for idx, hit in enumerate(searchHits['hits']):
                print("%s\t%s\t\t%s\t\t%s" % (idx + 1, hit['_score'], hit['_source']['title'], len(hit['_source']['overview'])))

    return httpResp

def explain(query):
    headers = {"Content-Type": "application/json"}
    httpResp = requests.get('http://localhost:9200/tmdb/_validate/query?explain',
                    headers=headers, data=json.dumps(query))
    pprint(json.loads(httpResp.text))
    
# query = {"field":"title", "text": "Fire with Fire"}

def analyze(index_name, query):
    resp = requests.get(f'http://localhost:9200/{index_name}/_analyze?format=yaml', 
                        headers=headers, data=json.dumps(query))
    print(resp.text)

def mappings(index_name):
    resp = requests.get(f'http://localhost:9200/{index_name}/_mappings?format=yaml')
    print(resp.text)

def refresh(index_name):
    resp = requests.get(f'http://localhost:9200/{index_name}/_refresh')
    print(resp.text)
