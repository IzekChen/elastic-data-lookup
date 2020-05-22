#!/bin/python3

from elasticsearch import Elasticsearch, exceptions
import json, time
import pandas as pd
import base64
from datetime import date, timedelta
import sys, getopt

def elastic_connection():
    client = Elasticsearch(
        ['10.62.1.100', '10.62.1.101', '10.62.1.179', '10.62.1.180', '10.62.1.181'],
        port=9200,
        maxsize=80
    )
    return client

def lists_of_day(argv):
    try:
        opts, args = getopt.getopt(argv, "d:", ["day"])
    except getopt.GetoptError:
        print("parameter format error")
        sys.exit(2)

    ddays = ''
    for opt, arg in opts:
        if opt in ("-d", "--day"):
            ddays = arg
    print(f"processing {ddays} days data")
    print(type(ddays))
    date_list = sorted(pd.date_range(pd.datetime.today()
                                    - pd.DateOffset(days=int(ddays)) ,
                                     periods=int(ddays)).tolist(), reverse=True)

    seperator = ","
    lists = ["mgplus-enriched-"+x.strftime("%Y.%m.%d") for x in date_list]
    lists = seperator.join(lists)
    print(lists)
    return lists

def ouid_decode(token):

    #print(type(token))
    missing_padding = 4 - len(token) % 4
    if missing_padding:
        token += '=' * missing_padding
    data = base64.b64decode(token)
    #print(type(data))
    data = data.decode('utf8').replace("'", '"')
    data = json.loads(data)
    #print(type(data))
    #print("json is: ", data)
    ouid = data.get('ouid')
    #print("Origin OUID is: ", ouid)
    ouid = ouid[2:]
    #print("stripped OUID: ", ouid)
    ouid = int(ouid, base=16)
    #print(ouid)
    #print(type(ouid))
    ouid = ((ouid >> 16) | ((65535 & ouid ) << 16))
    #print("decoded OUID: ", ouid)

    return ouid


def update_mg_account_details(doc_index, doc_id, ouid):

    doc_index = doc_index
    doc_id_ = doc_id
    ouid = ouid

    if es != None:
#        print(es.info())
        query_str =  {"query":{"bool":{"must":[{"match_phrase":{"player_account_uid": ouid}},{"exists":{"field":"mg_user_name"}}],"must_not":[{"terms":{"mg_gaming_server_id.keyword":["-1"]}}]}}}
        lookup_index = 'redshift-ref.player_account'
        res = es.search(index=lookup_index, body=query_str)
        total = res['hits']['total']
        if total > 0 :
            data = res['hits']['hits']
            for i in data:
                #print(i)
                mg_user_name = str(i['_source']['mg_user_name'])
                mg_gaming_server_id = str(i['_source']['mg_gaming_server_id'])
                mg_player_account_uid = str(i['_source']['player_account_uid'])
                mg_player_uid = str(i['_source']['player_uid'])
                mg_agent_name = str(i['_source']['agent_name'])
                mg_game_provider_code = str(i['_source']['game_provider_code'])
                mg_currency_isocode = str(i['_source']['currency_isocode'])
                mg_agent_player_id = str(i['_source']['agent_player_id'])
                mg_mg_user_id = str(i['_source']['mg_user_id'])
                mg_master_agent_uid = str(i['_source']['master_agent_uid'])
                mg_head_office_uid = str(i['_source']['head_office_uid'])
                mg_agent_uid = str(i['_source']['agent_uid'])
                mg_master_agent_name = str(i['_source']['master_agent_name'])
                mg_updated_date_utc = str(i['_source']['updated_date_utc'])
                mg_created_date_utc = str(i['_source']['created_date_utc'])
                mg_head_office_name = str(i['_source']['head_office_name'])
                mg_modified_datetime = str(i['_source']['modified_datetime'])
                print(
                    mg_user_name,
                    mg_gaming_server_id,
                    mg_player_account_uid,
                    mg_player_uid,
                    mg_agent_name,
                    mg_game_provider_code,
                    mg_currency_isocode,
                    mg_agent_player_id,
                    mg_mg_user_id,
                    mg_master_agent_uid,
                    mg_head_office_uid,
                    mg_agent_uid,
                    mg_master_agent_name,
                    mg_updated_date_utc,
                    mg_created_date_utc,
                    mg_head_office_name,
                    mg_modified_datetime
                )

            ### update index
            update_query =  {
                                "script":{
                                    "source":"ctx._source.mg_user_name = params.mg_user_name; ctx._source.mg_gaming_server_id = params.mg_gaming_server_id; ctx._source.mg_player_account_uid = params.mg_player_account_uid; ctx._source.mg_player_uid = params.mg_player_uid; ctx._source.mg_agent_name = params.mg_agent_name; ctx._source.mg_game_provider_code = params.mg_game_provider_code; ctx._source.mg_currency_isocode = params.mg_currency_isocode; ctx._source.mg_agent_player_id = params.mg_agent_player_id; ctx._source.mg_mg_user_id = params.mg_mg_user_id; ctx._source.mg_master_agent_uid = params.mg_master_agent_uid; ctx._source.mg_head_office_uid = params.mg_head_office_uid; ctx._source.mg_agent_uid = params.mg_agent_uid; ctx._source.mg_master_agent_name = params.mg_master_agent_name; ctx._source.mg_updated_date_utc = params.mg_updated_date_utc; ctx._source.mg_created_date_utc = params.mg_created_date_utc; ctx._source.mg_head_office_name = params.mg_head_office_name; ctx._source.mg_modified_datetime = params.mg_modified_datetime",
                                    "lang":"painless","params":
                                    {
                                        "mg_user_name":mg_user_name,
                                        "mg_gaming_server_id":mg_gaming_server_id,
                                        "mg_player_account_uid":mg_player_account_uid,
                                        "mg_player_uid": mg_player_uid,
                                        "mg_agent_name": mg_agent_name,
                                        "mg_game_provider_code": mg_game_provider_code,
                                        "mg_currency_isocode": mg_currency_isocode,
                                        "mg_agent_player_id": mg_agent_player_id,
                                        "mg_mg_user_id": mg_mg_user_id,
                                        "mg_master_agent_uid": mg_master_agent_uid,
                                        "mg_head_office_uid": mg_head_office_uid,
                                        "mg_agent_uid": mg_agent_uid,
                                        "mg_master_agent_name": mg_master_agent_name,
                                        "mg_updated_date_utc": mg_updated_date_utc,
                                        "mg_created_date_utc": mg_created_date_utc,
                                        "mg_head_office_name": mg_head_office_name,
                                        "mg_modified_datetime": mg_modified_datetime
                                    }
                                 }
                            }

#            print(type(update_query))
            update_query = json.dumps(update_query)
#            print(update_query)
            try:
                response = es.update(index=doc_index, doc_type="doc",id=doc_id, body=update_query)
                print('response', response)
            except Exception as e:
                print(e)

if __name__ == '__main__':

    ### initial connection
    es = elastic_connection()
    ### test elk connection
    try:

        info = json.dumps(es.info(), indent=4)
        print("Elasticsearch client info():", info)

    except exceptions.ConnectionError as err:
        print(err)
    if es != None:
        #date_list = lists_of_day(7)
        #query_str = '{"size": 1, "query":{"exists":{"field":"K2_GameLaunchToken"}}}'
        query_str = '{"query":{"bool":{"must":[{"exists":{"field":"K2_GameLaunchToken"}}],"must_not":[{"exists":{"field":"mg_head_office_name "}}]}}}'
        #mgplus_index = 'mgplus-enriched-2020.05.16'
        mgplus_index = lists_of_day(sys.argv[1:])
        res = es.search(index=mgplus_index, body=query_str, filter_path=['hits.hits._source', 'hits.hits._id','hits.hits._index','_scroll_id'], scroll = '30s')
        old_scroll_id = res['_scroll_id']
        while len(res['hits']['hits']):

            res = es.scroll(
                scroll_id = old_scroll_id,
                scroll = '30s'
            )

            data = res['hits']['hits']
            for i in data:
                #print("this is details: ", i)
                doc_id = i['_id']
                print("DocumentID: ", doc_id)
                K2_GameLaunchToken = i['_source']['K2_GameLaunchToken']
                if K2_GameLaunchToken:
                    doc_index = i['_index']
                    print("index: ", doc_index)
                    #print(K2_GameLaunchToken)
                    token = K2_GameLaunchToken.split('.')[1]
                    print("ouid: ", token)
                    ouid = ouid_decode(token)
                    data = update_mg_account_details(doc_index, doc_id, ouid)
#                    time.sleep(10)
                    time.sleep(0.05)
                else:
                    print("K2 Game Launch Token not exist")
                    time.sleep(0.5)


