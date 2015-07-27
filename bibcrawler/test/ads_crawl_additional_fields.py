#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
This script reads in a crawled ADS dataset and can be used to crawl additional fields that have been omitted in the
initial crawl
"""

from __future__ import print_function, division
import pandas as pd
import requests

base_directory = r"E:\Work\Know-Center\NASA_ADS\files\ads_data"
api_key = "vd6LgUN3DDe28Pmf"

headers = {'Authorization': "Bearer:" + api_key}
adsws_url = "http://adslabs.org/adsabs/api/search/"

def read_dataframe(filename):
    with open(filename, 'rb') as f:
        data = f.readlines()
    data = map(lambda x: x.rstrip(), data)
    data_json_str = "[" + ','.join(data) + "]"

    # Load json string
    data_df = pd.read_json(data_json_str)

    # # Data Preprocessing
    data_df.pubdate = map(lambda x: x[0:4], data_df.pubdate)
    data_df.pubdate = pd.to_datetime(data_df.pubdate, format="%Y")
    del data_df['aff']
    return data_df

cat = "Mathematics"
df = read_dataframe(r"E:\Work\Know-Center\NASA_ADS\files\ads_complete\{}.json".format(cat))
df = df[df.read_count != 0]

print(cat, len(df.index))
crawlsize=20#
crawl_indices = range(0, len(df.index), crawlsize)
crawl_indices.append((len(df.index)))

asd_ids = df['id'].tolist()
data = {}

for idx in range(len(crawl_indices)-1):
    ads_id_indices = range(crawl_indices[idx], crawl_indices[idx+1])
    crawl_ids = set([asd_ids[_] for _ in ads_id_indices])
    query = []
    for crawl_id in crawl_ids:
        query.append("id:{}+OR+".format(crawl_id))
    query = "".join(query)[0:-4]

    payload="q="+query+"&dev_key=Dk64Zg27E0v1by1w"


    r = requests.get(adsws_url, params=payload)
    try:
        r_json = r.json()
    except:
        print("Docs {} - {}: ".format(ads_id_indices[0], ads_id_indices[-1]), "Status ", r.status_code, " - Could not decode json")
        continue
    else:
        print("Docs {} - {}: ".format(ads_id_indices[0], ads_id_indices[-1]), "Status ", r.status_code, " - Hits: {}".format(len(r_json['results']['docs'])))
        for doc in r_json['results']['docs']:
            data[doc['id']] = {}
            if 'identifier' in doc:
                data[doc['id']]['identifier'] = doc['identifier']
            if 'page' in doc:
                data[doc['id']]['page'] = doc['page']

pd_df = pd.DataFrame(data)
pd_df.to_json(base_directory+"\\"+"{}.json".format(cat))