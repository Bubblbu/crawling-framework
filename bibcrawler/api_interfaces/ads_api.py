#!/usr/bin/python
# -*- coding: utf-8 -*-

from __future__ import print_function, division

import sys
import arrow
from path import Path

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
api_key = Config.get('ads', 'api_key')
adsws_url = Config.get('ads', 'ads_search_url')

headers = {'Authorization': "Bearer " + api_key}


def pretty_print_POST(req):
    """
    At this point it is completely built and ready
    to be fired; it is "prepared".

    However pay attention at the formatting used in
    this function because it is programmed to be pretty
    printed and may differ from the actual request.
    """
    print('{}\n{}\n{}\n\n{}'.format(
        '-----------START-----------',
        req.method + ' ' + req.url,
        '\n'.join('{}: {}'.format(k, v) for k, v in req.headers.items()),
        req.body,
    ))


class ADSThread(threading.Thread):
    def __init__(self, input_q, output_q, logger):
        threading.Thread.__init__(self)
        self.input_q = input_q
        self.output_q = output_q
        self.logger = logger

    def run(self):
        while not self.input_q.empty():
            count, payload = self.input_q.get_nowait()
            r = requests.get(adsws_url, params=payload, headers=headers)

            pretty_print_POST(requests.Request('GET', adsws_url, data=payload).prepare())

            self.logger.info("Request {} - Status: {}".format(count, r.status_code))
            try:
                temp = r.json()['response']['docs']
                self.output_q.put(temp)
            except Exception, e:
                print(e)


def ads_crawl_category(list_of_cats, number_of_docs=100, num_threads=1):
    """

    :param list_of_cats: <list> - Categories to crawl
    :param number_of_docs: <int> - Number of docs to crawl.
    :param num_threads: <int> - Number of ADS-Crawl threads to start
    :return: <str> - Working folder
    """

    timestamp = arrow.utcnow().to('Europe/Vienna').format('YYYY-MM-DD_HH-mm-ss')

    base_folder = base_directory

    working_folder = base_folder + timestamp
    Path(working_folder).mkdir()

    # Setup logging
    config = logging_confdict(working_folder, __name__)
    logging.config.dictConfig(config)
    ads_logger = logging.getLogger(__name__)

    ads_logger.info("\nCreated new folder: <<" + working_folder + ">>")

    input_queue = Queue.Queue()
    output_queue = Queue.Queue()

    for count, cat in enumerate(list_of_cats):
        payload = {'q': 'arxiv_class:"{}"'.format(cat), 'sort': 'read_count desc',
                   'fl': 'reader,title,abstract,'
                         'year,author,pub,read_count,'
                         'citation_count,identifier,arxiv_class,'
                         'primary_arxiv_class,arxiv_primary_class,'
                         'primary_class',
                   'rows': number_of_docs}

        input_queue.put((count, payload))

    threads = []
    for i in range(num_threads):
        thread = ADSThread(input_queue, output_queue, ads_logger)
        thread.start()
        threads.append(thread)

    ads_logger.debug("THREADING STARTED - PLEASE BE PATIENT")

    for thread in threads:
        thread.join()

    rows = []
    while not output_queue.empty():
        temp = output_queue.get_nowait()
        for doc in temp:
            # doc['url'] = "http://arxiv.org/abs/" + cat
            try:
                doc['authors'] = ";".join(doc['author'])
                del doc['author']
            except KeyError:
                doc['authors'] = []

            if 'reader' not in doc:
                doc['reader'] = []

            doc['readers'] = int(doc['read_count'])
            doc['reader_ids'] = u";".join(doc['reader'])
            doc['title'] = doc['title'][0]

            del doc['read_count']
            del doc['reader']
            rows.append(doc)

    # Convert to pandas dataframe
    df = pd.DataFrame(rows)

    # Rename columns
    df.rename(columns={'pub': 'published_in', 'abstract': 'paper_abstract'}, inplace=True)
    df.index.name = "id"

    # Output
    # ads_logger.debug("SAVING FILE")
    # df.to_csv(working_folder + "/ads_data.csv", sep=";", encoding='utf8', index=False)
    # df.to_json(working_folder + "/ads_data.json")

    return working_folder


def ads_crawl_dataset(input_folder=None, number_of_docs=100, num_threads=1):
    """
    Uses an existing dataframe containing arxiv_id's to crawl corresponding ADS data.
    Always uses the top *number_of_docs* documents for the search.

    :param input_folder: Input folder
    :param number_of_docs: Number of documents to use
    :param num_threads: Number of threads
    :return: Newly created working folder
    """
    timestamp = arrow.utcnow().to('Europe/Vienna').format('YYYY-MM-DD_HH-mm-ss')

    # Create folder structure
    if not input_folder:
        all_subdirs = [d for d in Path(base_directory).listdir() if d.isdir()]
        latest_subdir = max(all_subdirs, key=Path.getmtime)
        base_folder = latest_subdir + "/"
    else:
        # base_folder = base_directory + input_folder
        base_folder = input_folder
        if base_folder[-1] != "/":
            base_folder += "/"

    working_folder = base_folder + timestamp
    Path(working_folder).mkdir()

    # Setup logging
    config = logging_confdict(working_folder, __name__)
    logging.config.dictConfig(config)
    ads_logger = logging.getLogger(__name__)

    ads_logger.info("\nCreated new folder: <<" + working_folder + ">>")

    # Read in stage 1 file
    ads_logger.debug("\nReading in stage_3_raw.json ... (Might take a few seconds)")
    try:
        df = pd.read_json(base_folder + "/stage_3_raw.json")
    except IOError:
        ads_logger.exception("stage_3_raw.json does not exist")
        sys.exit()

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

        payload = {'q': 'arXiv:{}'.format(arxiv_id), 'sort': 'read_count desc'}

        input_queue.put((count, payload))

    threads = []
    for i in range(num_threads):
        thread = ADSThread(input_queue, output_queue, ads_logger)
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    rows = []
    while not output_queue.empty():
        temp = output_queue.get_nowait()[0]
        temp['url'] = "http://arxiv.org/abs/" + "none_currently"
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
        rows.append(temp)

    # Convert to pandas dataframe
    df = pd.DataFrame(rows)

    # Rename columns
    df.rename(columns={'pub': 'published_in', 'abstract': 'paper_abstract'}, inplace=True)
    df.index.name = "id"

    # Output
    df.to_csv(working_folder + "/ads_data.csv", Config.get("csv", "sep_char"),
              encoding='utf8', index=False)
    df.to_json(working_folder + "/ads_data.json")

    return working_folder


def clean_dataset(wd):
    pass


if __name__ == "__main__":
    ads_crawl_dataset(
        input_folder=r"E:\Work\Know-Center\CrawlingFramework\files\2015-07-23_17-11-25_qbio\2015-07-27_15-39-38\2015-07-27_16-16-35")