#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, division

from rpy2.robjects.packages import SignatureTranslatedAnonymousPackage
import rpy2.robjects as R
import rpy2.robjects.numpy2ri

rpy2.robjects.numpy2ri.activate()
from rpy2.robjects import pandas2ri

pandas2ri.activate()
import pandas.rpy.common as com
import numpy as np
import logging
import logging.config
from logging_dict import logging_confdict

import os, sys
import gc
import time
import datetime
import threading
import multiprocessing as mp
import Queue

import csv

from config import base_directory
from utils import *

__author__ = 'Asura Enkhbayar <asura.enkhbayar@gmail.com>'


class ProcessingThread(threading.Thread):
    def __init__(self, working_folder, input_queue, output_queue):
        threading.Thread.__init__(self)
        self.event = threading.Event()
        self.iq = input_queue
        self.oq = output_queue
        self.wdir = working_folder

    def run(self):
        print("running")
        with open(self.wdir + "/crossref_crawl_summary.csv", "ab") as f:
            writer = csv.writer(f, delimiter=";")
            while True:
                try:
                    result = self.iq.get_nowait()

                    lr = levenshtein_ratio(result['orig_title'], result['cr_title'])
                    if lr <= LR:
                        line = [result['index'],
                                result['cr_title'].encode('utf-8'),
                                result['orig_title'].encode('utf-8'),
                                result['cr_doi'].encode('utf-8'),
                                lr,
                                True]
                        result = {'idx': result['index'],
                                  'cr_title': result['cr_title'],
                                  'cr_doi': result['cr_doi'],
                                  'lr': lr}
                        print("FOUND DOI", lr)
                    else:
                        line = [result['index'],
                                result['cr_title'].encode('utf-8'),
                                result['orig_title'].encode('utf-8'),
                                result['cr_doi'].encode('utf-8'),
                                lr,
                                False]
                        result = {'idx': result['index'],
                                  'cr_title': np.nan,
                                  'cr_doi': np.nan,
                                  'lr': lr}

                    writer.writerow(line)
                    self.oq.put(result)
                    self.iq.task_done()

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
                print(self.idx)
                temp = com.convert_robj(self.doi_lookuper.crossref(self.author,
                                                                   self.title,
                                                                   self.date.strftime("%Y-%m-%d %H:%M:%S")))

                # Use index 1 because R is 1-indexed -> 0 is not the first element
                cr_title = temp['title'][1]
                if 'DOI' in temp:
                    cr_doi = temp['DOI'][1]
                else:
                    cr_doi = np.nan

                self.output_queue.put({'index': self.idx,
                                       'cr_title': unicode(cr_title.strip()),
                                       'orig_title': self.title.strip(),
                                       'cr_doi': cr_doi.strip()})

                # Garbage collection
                R.r('gc()')
                gc.collect()

                self.q.task_done()
            except Queue.Empty:
                if self.event.is_set():
                    break


def crossref_lookup(working_folder, index, authors, titles, submitted, num_threads=1):
    # Load r-scripts
    print("\nLoading R-Scripts ...")
    with open('r_scripts/doi_lookup.R', 'r') as f:
        string = ''.join(f.readlines())
    doi_lookuper = SignatureTranslatedAnonymousPackage(string, "doi_lookuper")

    cr_input_queue = Queue.Queue()
    cr_to_process = Queue.Queue()
    process_to_result = Queue.Queue()

    for idx, author, title, date in zip(index, authors, titles, submitted):
        tokens = author.split("|")
        if len(tokens) >= 15:
            author = "|".join(tokens[:15])
        cr_input_queue.put((idx, author, title, date))

    process_thread = ProcessingThread(working_folder, cr_to_process, process_to_result)

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


def doi_lookup(num_processes=1, num_threads=1, input_folder=None, mode='all'):
    """
    DOI Lookup interfaces to different DOI providers.
    Currently implemented: CrossRef.
    To-Do: DataCite

    Possible candidate documents are matched with original arxiv-documents using Levenshtein Ratio (Schloegl et al)

    :param input_folder: The folder containing the stage 1 data. If not given, the most recent folder will be used to work
    :type input_folder: str
    :param mode: The DOI Registration Agencies to be crawled
    :type mode: str

    :returns: pd.DataFrame - newly found DOIs with original indices
    """
    ts_start = time.time()
    timestamp = datetime.datetime.fromtimestamp(ts_start).strftime('%Y-%m-%d_%H-%M-%S')

    # Create folder structure
    global base_folder
    if not input_folder:
        all_subdirs = [base_directory + d for d in os.listdir(base_directory) if os.path.isdir(base_directory + d)]
        latest_subdir = max(all_subdirs, key=os.path.getmtime)
        base_folder = latest_subdir + "/"
    else:
        base_folder = base_directory + input_folder
        if base_folder[-1] != "/":
            base_folder += "/"

    working_folder = base_folder + timestamp
    os.mkdir(working_folder)

    with open(working_folder + "/crossref_crawl_summary.csv", "wb") as f:
        writer = csv.writer(f, delimiter=";")
        header = ["Index", "Title", "Orig_title", "DOI", "LR", "Match"]
        writer.writerow(header)

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
    else:
        stage_1.index = range(0, len(stage_1.index))

    # stage_1 = stage_1[300:700]

    stage_1['submitted'] = pd.to_datetime(stage_1['submitted'])
    stage_1['updated'] = pd.to_datetime(stage_1['updated'])

    df_ranges = range(0, len(stage_1.index), len(stage_1.index) // num_processes) + [len(stage_1.index)]
    pool_args = []
    for idx in range(num_processes):
        indices = stage_1[df_ranges[idx]:df_ranges[idx + 1]].index
        authors = stage_1[df_ranges[idx]:df_ranges[idx + 1]].authors
        titles = stage_1[df_ranges[idx]:df_ranges[idx + 1]].title
        submitted = stage_1[df_ranges[idx]:df_ranges[idx + 1]].submitted
        pool_args.append([indices, authors, titles, submitted, ])

    pool = mp.Pool(processes=num_processes)
    results = [pool.apply_async(crossref_lookup,
                                args=(working_folder, x[0], x[1], x[2], x[3], num_threads)) for x in pool_args]
    pool.close()
    pool.join()

    output = []
    for p in results:
        output.extend(p.get())

    # if mode == 'all':
    # results = crossref_lookup(stage_1.index,
    # stage_1.authors,
    # stage_1.title,
    # stage_1.submitted,
    #                               num_threads=num_workers)
    #
    # elif mode == 'crossref':
    #     results = crossref_lookup(stage_1.index,
    #                               stage_1.authors,
    #                               stage_1.title,
    #                               stage_1.submitted,
    #                               num_threads=num_workers)
    #
    # elif mode == 'datacite':
    #     pass

    cr_data = pd.DataFrame(output)
    cr_data = cr_data.set_index(["idx"])

    stage_2_raw = pd.merge(stage_1, cr_data, left_index=True, right_index=True, how='left')

    stage_2_raw.sort_index(inplace=True)

    try:
        stage_2_raw.to_json(working_folder + "/stage_2_raw.json")
        stage_2_raw.to_csv(working_folder + "/stage_2_raw.csv", encoding="utf-8", sep=";")
    except Exception, e:
        cr_logger.exception("Could not write all output files")
    else:
        cr_logger.info("Wrote json and csv output files")

    return working_folder


def doi_cleanup(working_folder, earliest_date=None, latest_date=None,
                remove_columns=None):
    """
    Cleans the crawl results from crossref.

    :param working_folder: Folder containing the files
    :type working_folder: str
    :param remove_columns: Columns to be removed from the crawled dataframe. If none given, default is None
    :type remove_columns: list of str
    :param earliest_date: Articles before this date are removed
    :type earliest_date: datetime
    :param latest_date: Artivles after this date are removed
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

    stage_2 = clean_dataset(stage_2_raw, cr_logger, earliest_date, latest_date, remove_columns)

    cr_unique_dois = stage_2.cr_doi.unique()
    arxiv_unique_dois = stage_2.doi.unique()
    common = set(cr_unique_dois) & set(arxiv_unique_dois)

    cr_logger.info("cr:{}, arxiv:{}, common:{}".format(len(cr_unique_dois), len(arxiv_unique_dois), len(common)))

    Noneless_stage2 = stage_2[[elem is not np.nan for elem in stage_2.cr_doi]]
    multiple_dois_bool = Noneless_stage2.cr_doi.duplicated()
    multiple_dois = Noneless_stage2[multiple_dois_bool].cr_doi

    bad_indices = []
    good_indices = []
    for count, bad_doi in enumerate(multiple_dois, start=1):
        temp = Noneless_stage2[[elem == bad_doi for elem in Noneless_stage2.cr_doi]]
        for idx, row in temp.iterrows():
            if row.doi is not np.nan:
                if row.doi.lower() == row.cr_doi.lower():
                    good_indices.append(idx)
                    continue
            bad_indices.append(idx)

    cr_logger.info("Kicked cr_doi - entries: {}".format(len(bad_indices)))
    cr_logger.info("Accepted cr_doi - entries: {}".format(len(good_indices)))

    for bad_idx in bad_indices:
        stage_2.loc[bad_idx].cr_doi = u"NOMATCH"

    stage_2.index = range(0, len(stage_2.index))

    try:
        stage_2.to_json(working_folder + "/stage_2.json")
        stage_2.to_csv(working_folder + "/stage_2.csv", encoding="utf-8", sep=";")
    except Exception, e:
        cr_logger.exception("Could not write all output files")
    else:
        cr_logger.info("Wrote json and csv output files")