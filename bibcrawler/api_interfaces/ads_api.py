#!/usr/bin/python
# -*- coding: utf-8 -*-

from __future__ import print_function, division

import os
import sys
import time
import datetime

import requests

import threading
import Queue

import pandas as pd

from utils import regex_new_arxiv, regex_old_arxiv

import logging
import logging.config
from logging_dict import logging_confdict

import configparser

Config = configparser.ConfigParser()
Config.read('../../config.ini')
base_directory = Config.get('directories', 'base')
api_key = Config.get('ads_auth', 'api_key')

headers = {'Authorization': "Bearer:" + api_key}
adsws_url = "http://adsws-staging.elasticbeanstalk.com/v1/search/query/"


class ADSThread(threading.Thread):
    def __init__(self, input_q, output_q, logger):
        threading.Thread.__init__(self)
        self.input_q = input_q
        self.output_q = output_q
        self.logger = logger

    def run(self):
        while not self.input_q.empty():
            count, arxiv_id = self.input_q.get_nowait()
            payload = {'q': 'arxiv:{}'.format(arxiv_id), 'sort': 'read_count desc',
                       'fl': 'reader,title,abstract,year,author,pub,read_count,citation_count,identifier'}
            r = requests.get(adsws_url, params=payload, headers=headers)
            self.logger.info("Request {} - Status: {}".format(count, r.status_code))
            temp = r.json()['response']['docs'][0]
            temp['url'] = "http://arxiv.org/abs/" + arxiv_id
            try:
                temp['authors'] = ";".join(temp['author'])
                del temp['author']
            except KeyError:
                temp['authors'] = []

            if 'reader' not in temp:
                temp['reader'] = []

            temp['readers'] = int(temp['read_count'])
            temp['reader_ids'] = u";".join(temp['reader'])
            temp['title'] = temp['title'][0]

            del temp['read_count']
            del temp['reader']
            self.output_q.put(temp)


def ads_crawl(input_folder=None, number_of_docs=100, num_threads=1):
    """

    :param input_folder: Input folder
    :return: Newly created working folder
    """
    ts_start = time.time()
    timestamp = datetime.datetime.fromtimestamp(ts_start).strftime('%Y-%m-%d_%H-%M-%S')

    # Create folder structure
    if not input_folder:
        all_subdirs = [base_directory + d for d in os.listdir(base_directory) if os.path.isdir(base_directory + d)]
        latest_subdir = max(all_subdirs, key=os.path.getmtime)
        base_folder = latest_subdir + "/"
    else:
        # base_folder = base_directory + input_folder
        base_folder = input_folder
        if base_folder[-1] != "/":
            base_folder += "/"

    working_folder = base_folder + timestamp
    os.mkdir(working_folder)

    # Setup logging
    config = logging_confdict(working_folder, __name__)
    logging.config.dictConfig(config)
    ads_logger = logging.getLogger(__name__)

    ads_logger.info("\nCreated new folder: <<" + working_folder + ">>")

    # Read in stage 1 file
    ads_logger.debug("\nReading in stage_1.json ... (Might take a few seconds)")
    try:
        df = pd.read_json(base_folder + "/stage_3_raw.json")
    except:
        ads_logger.exception("Problem occured while reading ")
        sys.exit("Could not read stage_1 file")

    df.sort(columns="reader_count", ascending=False, inplace=True)
    df.index = range(0, len(df.index))

    arxiv_ids = df['arxiv_id'][0:number_of_docs].tolist()

    input_queue = Queue.Queue()
    output_queue = Queue.Queue()

    for count, arxiv_id in enumerate(arxiv_ids):
        found_regex = regex_new_arxiv.findall(arxiv_id)
        if found_regex:
            arxiv_id = found_regex[0]
        else:
            found_regex = regex_old_arxiv.findall(arxiv_id)
            if found_regex:
                arxiv_id = found_regex[0]

        input_queue.put((count, arxiv_id))

    threads = []
    for i in range(num_threads):
        thread = ADSThread(input_queue, output_queue, ads_logger)
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    rows = []
    while not output_queue.empty():
        rows.append(output_queue.get_nowait())

    # Convert to pandas dataframe
    df = pd.DataFrame(rows)

    # Rename columns
    df.rename(columns={'pub': 'published_in', 'abstract': 'paper_abstract'}, inplace=True)
    df.index.name = "id"

    # Output
    df.to_csv(working_folder + "/ads_data.csv", sep=";", encoding='utf8', index=False)
    df.to_json(working_folder + "/ads_data.json")

    return working_folder