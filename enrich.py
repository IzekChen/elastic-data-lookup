#!/bin/python3

from datetime import datetime
from elasticsearch import Elasticsearch, exceptions
import json, time
import pandas as pd

def elastic_connection():
    client = Elasticsearch(
        ['10.62.1.100', '10.62.1.101', '10.62.1.179', '10.62.1.180', '10.62.1.181'],
        port=9200,
        maxsize=80
    )

#    print("yes") if client else print("es connect failed")
    return client

def elastic_lookup(k2_playername,doc_id, doc_index):
    doc_index = doc_index
    lookup_index = "redshift-player_account"
    query_str = {"query":{"bool":{"must":[{"match_phrase":{"agent_player_id": k2_playername}},{"exists":{"field":"mg_user_name"}}]}}}
    #query_str = {"query":{"bool":{"must":[{"match_phrase":{"agent_player_id":"5C2A2020043038117"}}]}}}  ### 8Z7W83EBbMsZVJAiHnHc
    q = json.dumps(query_str)

    res = es.search(index=lookup_index, body=q)

    total = res['hits']['total']

    if total != 0:
        data = res['hits']['hits']
        #print('data: ', data)
        for i in data:
            #print(i)
            mg_user_name = str(i['_source']['mg_user_name'])
            mg_gaming_server_id = str(i['_source']['mg_gaming_server_id'])

            print(mg_user_name)
            print(mg_gaming_server_id)
            if mg_user_name and mg_gaming_server_id:
                return mg_user_name,mg_gaming_server_id
    else:
        print("Not matched")
        return None, None

def elastic_update(mg_user_name, doc_id, doc_index, mg_gaming_server_id):


    #index = "mgplus-enriched*"
    #doc_id = '6266228326761949699'
    doc_index = doc_index
    doc_id = doc_id
    mg_user_name = mg_user_name
#    query = {"script":{"source": 'ctx._source.mg_user_name = "' + mg_user_name + '"; ctx._source.mg_gaming_server_id = "' + mg_gaming_server_id +'"'}}
    query = '{"script":{"source":"ctx._source.mg_user_name = params.mg_user_name; ctx._source.mg_gaming_server_id = params.mg_gaming_server_id","lang":"painless","params":{"mg_user_name":"%s","mg_gaming_server_id":"%s"}}}' % (mg_user_name, mg_gaming_server_id)
#            {'script': {'source': 'ctx._source.mg_user_name = "M0G109019861"; ctx._source.mg_gaming_server_id = "555"\''}}
    print(query)
    #query = {"script":{"source":"ctx._source.test = '123'; ctx._source.test1 = '1234'"}}
    try:
        response = es.update(index=doc_index, doc_type="doc",id=doc_id, body=query )
        print('response', response)
    except Exception as e:
        print(e)

if __name__ == '__main__':
    es = elastic_connection()


    try:
        # use the JSON library's dump() method for indentation
        info = json.dumps(es.info(), indent=4)

        # pass client object to info() method
        print ("Elasticsearch client info():", info)

    except exceptions.ConnectionError as err:

        # print ConnectionError for Elasticsearch
        print ("\nElasticsearch info() ERROR:", err)
        print ("\nThe client host:", host, "is invalid or cluster is not running")

        # change the client's value to 'None' if ConnectionError
        es = None

    if es != None:
        query_str = '{"query":{"exists":{"field":"K2_PlayerName"}}}'
#        res = es.search(index="mgplus-enriched-2020.05.12", body=query_str, filter_path=['hits.hits._source', 'hits.hits._id','hits.hits._index','_scroll_id'], scroll = '30s')
        res = es.search(index="mgplus-enriched-2020.05*", body=query_str, filter_path=['hits.hits._source', 'hits.hits._id','hits.hits._index','_scroll_id'], scroll = '30s')
        old_scroll_id = res['_scroll_id']
        while len(res['hits']['hits']):

            res = es.scroll(
                scroll_id = old_scroll_id,
                scroll = '30s'
            )

            data = res['hits']['hits']
            for i in data:
                doc_id = i['_id']
                k2_playername = i['_source']['K2_PlayerName']
                if k2_playername:
                    doc_index = i['_index']
                    print(doc_index, doc_id, k2_playername)
                    mg_user_name, mg_gaming_server_id = elastic_lookup(k2_playername, doc_id, doc_index)
                    #elastic_lookup(k2_playername, doc_id, doc_index)
                    #mg_user_name, mg_gaming_server_id = elastic_lookup('a026_wang197982', '8818620452793831332', 'mgplus-enriched-2020.05.13')
                    print('mg_user_name: ', mg_user_name)
                    print('mg_gaming_server_id: ', mg_gaming_server_id)
                    if mg_user_name :
                        elastic_update(mg_user_name, doc_id, doc_index, mg_gaming_server_id)
#                    time.sleep(0.1)

#        elastic_update('OCP023871752', '8818620452839575889', 'mgplus-enriched-2020.05.13')