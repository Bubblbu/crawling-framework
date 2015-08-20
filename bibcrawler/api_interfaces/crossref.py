#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This module provides multiprocessed crossref crawling. Multiprocessing -> not so nice logging... TODO
"""

from __future__ import print_function, division

import os
import sys
import time
import datetime
import threading
import multiprocessing as mp
import Queue
from path import Path

import numpy as np
import pandas as pd

from rpy2.robjects.packages import SignatureTranslatedAnonymousPackage
import rpy2.robjects as rpy2_objects
from rpy2.robjects import pandas2ri, numpy2ri

numpy2ri.activate()
pandas2ri.activate()

import gc
import csv

import logging
import logging.config
from logging_dict import logging_confdict

import configparser

Config = configparser.ConfigParser()
Config.read('../../config.ini')
base_directory = Config.get('directories', 'base')

from utils import levenshtein_ratio, LR, clean_dataset


# Helper functions
def get_crawl_speed(average_speed, smoothing_factor, last_speed):
    return smoothing_factor * last_speed + (1 - smoothing_factor) * average_speed


def get_hms(seconds):
    eta_h = int(seconds / 60 / 60)
    eta_m = int(seconds / 60 - eta_h * 60)
    eta_s = int(seconds - eta_h * 3600 - eta_m * 60)
    return eta_h, eta_m, eta_s


def get_eta(average_speed, tps, doc_count, progress_count, eta_weight=0.2):
    eta_h, eta_m, eta_s = get_hms((doc_count - progress_count) * average_speed)
    tt_eta_h, tt_eta_m, tt_eta_s = get_hms(tps / progress_count * doc_count - tps)
    eta_h = eta_weight * eta_h + (1 - eta_weight) * tt_eta_h
    eta_m = eta_weight * eta_m + (1 - eta_weight) * tt_eta_m
    eta_s = eta_weight * eta_s + (1 - eta_weight) * tt_eta_s

    return eta_h, eta_m, eta_s


# Threads
class ProcessingThread(threading.Thread):
    def __init__(self, working_folder, input_queue, output_queue, doc_count):
        threading.Thread.__init__(self)
        self.event = threading.Event()
        self.iq = input_queue
        # self.oq = output_queue
        self.wdir = working_folder
        self.doc_count = doc_count
        self.progress_count = 1
        self.average_speed = None
        self.pid = os.getpid()

    def run(self):
        print("running")
        with open(self.wdir + "/temp/proces_{}.csv".format(self.pid), "ab") as f:
            writer = csv.writer(f, delimiter=";")
            tts = time.time()
            while True:
                try:
                    ts = time.time()
                    result = self.iq.get_nowait()
                    lr = levenshtein_ratio(result['orig_title'], result['cr_title'])

                    line = [result['index'],
                            result['cr_title'].encode('utf-8'),
                            result['orig_title'].encode('utf-8'),
                            result['cr_doi'].encode('utf-8'),
                            lr]

                    if lr <= LR:
                        line.append(True)
                    else:
                        line.append(False)

                    writer.writerow(line)
                    f.flush()
                    # self.oq.put(result)
                    self.iq.task_done()

                    passed_time = time.time() - ts
                    tps = time.time() - tts
                    if not self.average_speed:
                        self.average_speed = passed_time
                    else:
                        self.average_speed = get_crawl_speed(self.average_speed, 0.8, passed_time)

                    eta_h, eta_m, eta_s = get_eta(self.average_speed, tps, self.doc_count, self.progress_count)

                    print("PID {}: {}/{} - ETA: {}h {:0>2}m {:0>2}s".format(self.pid,
                                                                            self.progress_count, self.doc_count,
                                                                            int(eta_h), int(eta_m), int(eta_s)))
                    self.progress_count += 1

                except Queue.Empty:
                    if self.event.is_set():
                        break


class CrossrefAPIThread(threading.Thread):
    def __init__(self, queue, output_queue, doi_lookuper):
        threading.Thread.__init__(self)
        self.event = threading.Event()
        self.q = queue
        self.output_queue = output_queue
        self.doi_lookuper = doi_lookuper

        self.idx = None
        self.author = None
        self.title = None
        self.date = None

    def run(self):
        while True:
            try:
                self.idx, self.author, self.title, self.date = self.q.get_nowait()
                temp = pandas2ri.ri2py(self.doi_lookuper.crossref(self.author,
                                                                  self.title,
                                                                  self.date.strftime("%Y-%m-%d %H:%M:%S")))

                try:
                    cr_title = temp['title'][0]
                except KeyError:
                    cr_title = ""
                try:
                    cr_doi = temp['DOI'][0]
                except KeyError:
                    cr_doi = ""

                self.output_queue.put({'index': self.idx,
                                       'cr_title': unicode(cr_title.strip()),
                                       'orig_title': self.title.strip(),
                                       'cr_doi': cr_doi.strip()})

                # Garbage collection
                rpy2_objects.r('gc()')
                gc.collect()

                self.q.task_done()
            except Queue.Empty:
                if self.event.is_set():
                    break


def crossref_lookup(working_folder, index, authors, titles, submitted, num_threads=1):
    # Load r-scripts
    print("\nLoading R-Scripts ...")
    with open('../r_scripts/doi_lookup.R', 'r') as f:
        string = ''.join(f.readlines())
    doi_lookuper = SignatureTranslatedAnonymousPackage(string, "doi_lookuper")

    cr_input_queue = Queue.Queue()
    cr_to_process = Queue.Queue()
    process_to_result = Queue.Queue()

    doc_count = 0
    for idx, author, title, date in zip(index, authors, titles, submitted):
        tokens = author.split("|")
        if len(tokens) >= 15:
            author = "|".join(tokens[:15])
        cr_input_queue.put((idx, author, title, date))
        doc_count += 1

    process_thread = ProcessingThread(working_folder, cr_to_process, process_to_result, doc_count)

    print("\nStarting crossref crawl process...")
    crossref_threads = []
    for i in range(num_threads):
        thread = CrossrefAPIThread(cr_input_queue, cr_to_process, doi_lookuper)
        thread.start()
        crossref_threads.append(thread)

    process_thread.start()

    for thread in crossref_threads:
        thread.event.set()

    for thread in crossref_threads:
        thread.join()

    process_thread.event.set()

    process_thread.join()

    results = []
    while not process_to_result.empty():
        results.append(process_to_result.get())

    return results


def crossref_crawl(num_processes=1, num_threads=1, input_folder=None, continue_folder = None):
    """
    DOI Lookup interfaces to different DOI providers.
    Currently implemented: CrossRef.
    To-Do: DataCite

    Stage 1 dataset is split into equally sized subframes. Each is given to a subprocess that accesses the
    crossref API with multiple threads.
    Possible candidate documents are matched with original arxiv-documents using Levenshtein Ratio (Schloegl et al)
    
    :param num_processes: Number of processes to split the initial stage_1_dataset
    :param num_threads: Number of threads each process uses to access crossref API
    :param input_folder: The folder containing the stage 1 data. If not given, the most recent folder will be used to work
    :returns: pd.DataFrame - newly found DOIs with original indices
    """
    ts_start = time.time()
    timestamp = datetime.datetime.fromtimestamp(ts_start).strftime('%Y-%m-%d_%H-%M-%S')

    # Create folder structure
    if not input_folder:
        all_subdirs = [d for d in Path(base_directory).listdir() if d.isdir()]
        latest_subdir = max(all_subdirs, key=Path.getmtime)
        base_folder = latest_subdir + "/"
    else:
        base_folder = input_folder
        if base_folder[-1] != "/":
            base_folder += "/"

    if continue_folder:
        working_folder = continue_folder
        temp_folder = working_folder + "/temp/"
    else:
        working_folder = base_folder + timestamp
        temp_folder = working_folder + "/temp/"
        Path(working_folder).mkdir()
        Path(temp_folder).mkdir()

    skip_indices = set()
    if continue_folder:
        # Setup logging
        config = logging_confdict(working_folder, __name__)
        logging.config.dictConfig(config)
        cr_logger = logging.getLogger(__name__)

        cr_logger.info("Continuing crawl in <<" + working_folder + ">>")

        for temp_file in Path(temp_folder).files("*.csv"):
            with open(temp_file, "rb") as tempfile:
                r = csv.reader(tempfile, delimiter=";")
                for line in r:
                    if len(line) == 6:
                        if line[-1] == "False" or line[-1] == "True":
                            skip_indices.add(int(line[0]))

    else:
        # Setup logging
        config = logging_confdict(working_folder, __name__)
        logging.config.dictConfig(config)
        cr_logger = logging.getLogger(__name__)

        cr_logger.info("\nCreated new folder: <<" + working_folder + ">>")

    # Read in stage 1 file
    cr_logger.debug("\nReading in stage_1.json ... (Might take a few seconds)")
    try:
        stage_1 = pd.read_json(base_folder + "/stage_1.json")
    except:
        cr_logger.exception("Problem occured while reading ")
        sys.exit("Could not read stage_1 file")

    stage_1.sort_index(inplace=True)
    stage_1['submitted'] = pd.to_datetime(stage_1['submitted'], unit="ms")

    stage_1.index = range(0, len(stage_1.index))

    crawl_stage_1 = stage_1.drop(skip_indices)

    cr_logger.info("\nSpawning {} processes - output will be cluttered... :S\n".format(num_processes))
    # Split df into n sub-dataframes for n processes
    df_ranges = range(0, len(crawl_stage_1.index), len(crawl_stage_1.index) // num_processes+1)
    df_ranges = df_ranges + [len(crawl_stage_1.index)]
    pool_args = []
    if len(df_ranges) == 1:
        indices = []
        authors = []
        titles = []
        submitted = []
        pool_args.append([indices, authors, titles, submitted])
    else:
        for idx in range(num_processes):
            cr_logger.info("Starting process {}".format(idx))
            indices = crawl_stage_1.iloc[range(df_ranges[idx], df_ranges[idx + 1])].index.values
            authors = crawl_stage_1.iloc[range(df_ranges[idx], df_ranges[idx + 1])].authors
            titles = crawl_stage_1.iloc[range(df_ranges[idx], df_ranges[idx + 1])].title
            submitted = crawl_stage_1.iloc[range(df_ranges[idx], df_ranges[idx + 1])].submitted
            pool_args.append([indices, authors, titles, submitted])

    pool = mp.Pool(processes=num_processes)
    for x in pool_args:
        pool.apply_async(crossref_lookup, args=(working_folder, x[0], x[1], x[2], x[3], num_threads))

    pool.close()
    pool.join()

    cr_logger.info("All processes finished")

    output = []
    for temp_file in Path(temp_folder).files("*.csv"):
        with open(temp_file, "rb") as tempfile:
            r = csv.reader(tempfile, delimiter=";")
            for line in r:
                if len(line) == 6:
                    result = {'idx': int(line[0]),
                              'cr_title': line[1],
                              'cr_doi': line[3],
                              'lr': line[4]}
                    if line[-1] == "False":
                        result['cr_title'] = np.nan
                        result['cr_doi'] = np.nan
                    output.append(result)

    cr_data = pd.DataFrame(output)
    cr_data = cr_data.set_index("idx", drop=True)

    cr_logger.info("\nMerging stage_1 dataset and crossref results")

    stage_2_raw = pd.merge(stage_1, cr_data, left_index=True, right_index=True, how='left')
    print(stage_2_raw)
    stage_2_raw.sort_index(inplace=True)

    try:
        stage_2_raw.to_json(working_folder + "/stage_2_raw.json")
        stage_2_raw.to_csv(working_folder + "/stage_2_raw.csv", encoding="utf-8",
                           sep=Config.get("csv", "sep_char"), index=False)
    except Exception, e:
        cr_logger.exception("Could not write all output files")
    else:
        cr_logger.info("Wrote stage_2_raw json and csv output files")

    return working_folder


def crossref_cleanup(working_folder, earliest_date=None, latest_date=None,
                     remove_columns=None):
    """
    Cleans the crawl results from crossref.

    :param working_folder: Folder containing the files
    :type working_folder: str
    :param remove_columns: Columns to be removed from the crawled dataframe. If none given, default is None
    :type remove_columns: list of str
    :param earliest_date: Articles before this date are removed
    :type earliest_date: datetime
    :param latest_date: Articles after this date are removed
    :type latest_date: datetime

    :return: None
    """

    config = logging_confdict(working_folder, __name__ + "_cleanup")
    logging.config.dictConfig(config)
    cr_logger = logging.getLogger(__name__ + "_cleanup")

    # Read in stage_1 raw file
    try:
        stage_2_raw = pd.read_json(working_folder + "/stage_2_raw.json")
    except Exception, e:
        cr_logger.exception("Could not load stage_1_raw file")
        sys.exit("Could not load stage 2 raw")
    else:
        cr_logger.info("Stage_1_raw successfully loaded")

    if not remove_columns:
        remove_columns = eval(Config.get('data_settings', 'remove_cols'))
    stage_2 = clean_dataset(stage_2_raw, cr_logger, earliest_date, latest_date, remove_columns)

    cr_unique_dois = stage_2.cr_doi.unique()
    arxiv_unique_dois = stage_2.doi.unique()
    common = set(cr_unique_dois) & set(arxiv_unique_dois)

    cr_logger.info("cr:{}, arxiv:{}, common:{}".format(len(cr_unique_dois), len(arxiv_unique_dois), len(common)))

    stage_2_no_nan = stage_2[[elem is not np.nan for elem in stage_2.cr_doi]]
    multiple_dois_bool = stage_2_no_nan.cr_doi.duplicated()
    multiple_dois = stage_2_no_nan[multiple_dois_bool].cr_doi

    bad_indices = []
    good_indices = []
    for count, bad_doi in enumerate(multiple_dois, start=1):
        temp = stage_2_no_nan[[elem == bad_doi for elem in stage_2_no_nan.cr_doi]]
        for idx, row in temp.iterrows():
            if row.doi is not np.nan:
                if row.doi.lower() == row.cr_doi.lower():
                    good_indices.append(idx)
                    continue
            bad_indices.append(idx)

    cr_logger.info("Kicked cr_doi - entries: {}".format(len(bad_indices)))
    cr_logger.info("Accepted cr_doi - entries: {}".format(len(good_indices)))

    for bad_idx in bad_indices:
        stage_2.loc[bad_idx, ['cr_doi']] = u"NO_DOI_MATCH"

    stage_2.index = range(0, len(stage_2.index))

    try:
        stage_2.to_json(working_folder + "/stage_2.json")
        stage_2.to_csv(working_folder + "/stage_2.csv", encoding="utf-8",
                       sep=Config.get("csv", "sep_char"), index=False)

    except Exception, e:
        cr_logger.exception("Could not write all output files")
    else:
        cr_logger.info("Wrote stage-2 cleaned json and csv output files")
