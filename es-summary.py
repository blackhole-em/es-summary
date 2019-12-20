import coloredlogs
import argparse
import requests
import json
import pprint
import logging
import collections
from pathos.multiprocessing import Pool
from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk
from datetime import timedelta
from datetime import datetime
yesterday = datetime.now() - timedelta(minutes=1440)
yest = yesterday.strftime('%Y%m%d')

def update(d, u):
    for k, v in u.items():
        if isinstance(v, collections.Mapping):
            d[k] = update(d.get(k, {}), v)
        else:
            d[k] = v
    return d
def update_template(turl, template):
    headers = {'Accept': 'application/json', 'Content-type':'application/json'}
    #query=json.dumps(template)
    resp = requests.put(turl, data=template, headers=headers, verify=False)
    rjson = resp.json()
    print(rjson)
    return rjson


def query_es(url, myquery):
    headers = {'Accept': 'application/json', 'Content-type':'application/json'}
    query=json.dumps(myquery)
    resp = requests.post(url, data=query, headers=headers, verify=False)
    rjson = resp.json()
    return rjson
    if 'hits' not in rjson:
        logging.warn("No Hits in Elasticsearch response")
        return {}
    else:
        ans = rjson["hits"].get("hits", {})
        logging.info("%d hits from ElasticSearch", len(ans))
        return ans

def generate_actions(data,myindex):
    now = datetime.now()
    today = now.strftime("%Y%m%d")
    #myindex = "indexname"
    for result in data:
        yield {
            '_op_type': 'index',
            '_index': myindex,
            '_type': '_doc',
            '_source': result
           }

def push_to_es(myes,data,myindex):
    es = Elasticsearch(myes)
    for success, info in parallel_bulk(es, generate_actions(data,myindex)):
        if not success: logging.info('Document failed', info)

def get_es_aggs(myesurl, myesdata, summary_results):
    sdata = query_es(myesurl, myesdata)
    if 'aggregations' in sdata:
        for hit in sdata["aggregations"]["distinct_facets"]["buckets"]:
            hkey = hit["key"]
            hkey.update({"doc_count":hit["doc_count"]})
            summary_results.append(hkey)
        if 'after_key' in sdata["aggregations"]["distinct_facets"]:
            update_after = {"aggs":{"distinct_facets":{"composite":{ "after" : sdata["aggregations"]["distinct_facets"]["after_key"]}}}}
            updatedesdata = update(myesdata, update_after)
            get_es_aggs(myesurl, myesdata, summary_results)
    return summary_results

def process_summary(mysummary):
    summary = mysummary["summary"]
    myes = mysummary["myes"]
    stunit = summary["time_ago_unit"]
    sago = summary["time_ago"]
    srctime = datetime.now() - timedelta(days=int(sago))
    stime = srctime.strftime(summary["source_datemask"])
    myindex = summary["source"].replace("$source_datemask",stime)
    myesurl = myes + str('/') + str(myindex) + '/_search?pretty&size=0'
    myesdata = { "aggs": { "distinct_facets": { "composite": { "sources": [], "size": 10000 } } } }
    desttime = summary["dest_time"]
    desttimeunit = summary["dest_time_unit"]
    detime = datetime.now() - timedelta(days=int(desttime))
    dtime = detime.strftime(summary["dest_datemask"])
    d_index = summary["dest_index"]+str(dtime)
    template_url = str(myes)+'/_template/'+str(summary["dest_template_name"])
    tempcheck = requests.get(template_url)
    logging.debug('template check - '+str(tempcheck))
    if tempcheck.status_code != 200:
        try:
            if eswrite:
                logging.info('No template found for '+str(d_index)+'.  Adding template from config')
                tempupdate = update_template(template_url,summary["dest_template"])
            else:
                logging.info('No template found for '+str(d_index)+'.  Not adding config (no --enable-write)')
        except Exception as e:
            logging.error('Failed to update the ES template: '+str(e))
    for fkey, fval in summary["fields"].items():
        myesdata["aggs"]["distinct_facets"]["composite"]["sources"].append({fkey :{"terms":{"field": fval}}})
    summary_results = []
    ## Get composite aggregations
    es_aggs = get_es_aggs(myesurl, myesdata, summary_results)
    ## Get stats
    src_index_count = query_es(myesurl.replace("_search","_count").replace("&size=0",""), {})["count"]
    doc_reduction = (int(len(es_aggs)) / int(src_index_count))
    mystats = { "fields": len(summary["fields"]), "source_index":myindex, "source_count":src_index_count, "summarized_count":len(es_aggs), "percent_reduced": doc_reduction }
    logging.info ('mystats = '+str(mystats))
    ## Write to ES
    if eswrite:
        try:
            logging.debug('Pushing data into Elastic - '+str(myes)+'/'+str(d_index))
            push_to_es(myes, es_aggs,d_index)
            requests.get(myes+'/'+d_index+'/_flush')
            requests.get(myes+'/'+d_index+'/_forcemerge?max_num_segments=1')
            isize = requests.get(myes+'/'+myindex+'/_stats').json()["_all"]["primaries"]["store"]["size_in_bytes"]
            osize = requests.get(myes+'/'+d_index+'/_stats').json()["_all"]["primaries"]["store"]["size_in_bytes"]
            opercent = (1 - (osize / isize))
            d_index_count = requests.get(myes+'/'+str(d_index)+'/_count').json()["count"]
            if len(es_aggs) == d_index_count:
                logging.info('All summary documents created successfully')
            else:
                logging.error('Not all documents were successfully created!!')
            logging.info('insize='+str(isize)+' - osize='+str(osize))
            logging.info('size reduced by %'+str(opercent))
            logging.info('src_docs='+str(len(es_aggs))+' dest_docs='+str(d_index_count))

        except Exception as e:
            logging.error('Unable to push data into Elastic - '+str(e))


configfile="config.json"

if __name__ == "__main__":
    Config = {}
    summaries = []
    argparser = argparse.ArgumentParser()
    argparser.add_argument('--enable-write',action="store_true",help="This is to actually write data to ES")
    args = argparser.parse_args()
    eswrite = args.enable_write
    ## Load config file
    with open(configfile, 'r') as f:
        Config = json.load(f)
    ## Set the logging level
    loglevel = Config["LogLevel"]
    logging.getLogger('elasticsearch').setLevel(logging.WARN)
    logging.basicConfig(level=logging.INFO)
    for cluster in Config["clusters"]:
        for skey, svalue in cluster.items():
            myes = svalue["ES"]["Server"]
            for summary in svalue["Summarization"]:
                logging.debug("Summary = "+str(summary))
                summaries.append({"myes":myes, "summary":summary})
    ## Multiprocessing
    pool = Pool(2)
    pls = pool.map(process_summary,summaries)
