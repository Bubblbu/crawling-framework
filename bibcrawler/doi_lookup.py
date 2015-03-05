#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, division

from rpy2.robjects.packages import SignatureTranslatedAnonymousPackage
import pandas.rpy.common as com
import pandas as pd
import numpy as np

import os
import time
import datetime
import threading
import Queue

from config import base_directory
from utils import levenshtein_ratio, LR

__author__ = 'Asura Enkhbayar <asura.enkhbayar@gmail.com>'


class CrossrefThread(threading.Thread):
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
                self.idx, self.author, self.title, self.date = self.q.get(timeout=1)
                print(self.idx)

                temp = com.convert_robj(self.doi_lookuper.crossref(self.author, self.title, self.date))
                temp['order'] = self.idx
                temp['orig_title'] = self.title
                self.output_queue.put(temp)
            except Queue.Empty:
                if self.event.is_set():
                    break
            except Exception, e:
                print(str(e))


def crossref_lookup(index, authors, titles, submitted, num_threads=1):
    # Load r-scripts
    print("\nLoading R-Scripts ...")
    with open('r_scripts/doi_lookup.R', 'r') as f:
        string = ''.join(f.readlines())
    doi_lookuper = SignatureTranslatedAnonymousPackage(string, "doi_lookuper")

    input_queue = Queue.Queue()
    output_queue = Queue.Queue()

    cr_lookup = pd.DataFrame()
    for idx, author, title, date in zip(index, authors, titles, submitted):
        tokens = author.split("|")
        if len(tokens) >= 15:
            author = "|".join(tokens[:15])
        input_queue.put((idx, author, title, date))

    crossref_threads = []

    print("\nStarting crossref crawl process...")
    for i in range(num_threads):
        thread = CrossrefThread(input_queue, output_queue, doi_lookuper)
        thread.start()
        crossref_threads.append(thread)

    for thread in crossref_threads:
        thread.event.set()

    for thread in crossref_threads:
        thread.join()

    while True:
        try:
            df = output_queue.get_nowait()
            cr_lookup = pd.concat([cr_lookup, df])
        except Queue.Empty:
            break

    cr_lookup = cr_lookup.sort(columns=['order'], ascending=True)
    cr_lookup.index = range(0, len(cr_lookup.index))

    cr_dois = []
    lr_results = []
    cr_titles = []

    print("\nChecking titles with LR")
    for idx, (original, found, doi) in enumerate(zip(cr_lookup.orig_title, cr_lookup.title, cr_lookup.DOI)):
        print(idx)
        lr = levenshtein_ratio(original, found)

        if lr <= LR:
            cr_dois.append(doi)
            cr_titles.append(found)
        else:
            cr_dois.append(np.nan)
            cr_titles.append(np.nan)

        lr_results.append(lr)

    return cr_dois, lr_results, cr_titles


def doi_lookup(num_workers=1, stage1_dir=None, mode='all'):
    """
    DOI Lookup interfaces to different DOI providers.
    Currently implemented: CrossRef.
    To-Do: DataCite

    Possible candidate documents are matched with original arxiv-documents using Levenshtein Ratio (Schloegl et al)

    :param stage1_dir: The folder containing the stage 1 data. If not given, the most recent folder will be used to work
    :type stage1_dir: str
    :param mode: The DOI Registration Agencies to be crawled
    :type mode: str

    :returns: pd.DataFrame - newly found DOIs with original indices
    """
    ts_start = time.time()
    timestamp = datetime.datetime.fromtimestamp(ts_start).strftime('%Y-%m-%d_%H-%M-%S')

    # Create folder structure
    if not stage1_dir:
        all_subdirs = [base_directory + d for d in os.listdir(base_directory) if os.path.isdir(base_directory + d)]
        latest_subdir = max(all_subdirs, key=os.path.getmtime)
        stage1_dir = latest_subdir + "/"
        print("\nStage-1 directory was not given. Latest directory has been chosen:\n<<" + stage1_dir + ">>")
    else:
        stage1_dir = base_directory + stage1_dir
        if stage1_dir[-1] != "/":
            stage1_dir += "/"
        print("\nStage-1 directory was chosen:\n\n<<" + stage1_dir + ">>")

    working_folder = stage1_dir + timestamp
    os.makedirs(working_folder)

    print("\nCreated new folder: <<" + working_folder + ">>")

    # Read in stage 1 file
    print("\nReading in stage_1.json ... (Might take a few seconds)")
    df = pd.io.json.read_json(stage1_dir + "stage_1.json")
    df.index = range(0, len(df.index))

    # Crawl additional dois
    cr_dois = []
    lr_results = []
    cr_titles = []

    if mode == 'all':
        cr_dois, lr_results, cr_titles = crossref_lookup(df.index, df.authors, df.title, df.submitted,
                                                         num_threads=num_workers)

    elif mode == 'crossref':
        cr_dois, lr_results, cr_titles = crossref_lookup(df.index, df.authors, df.title, df.submitted,
                                                         num_threads=num_workers)

    elif mode == 'datacite':
        pass

    df['crossref_doi'] = pd.Series(cr_dois)
    df['levenshtein_ratio'] = pd.Series(lr_results)
    df['crossref_title'] = pd.Series(cr_titles)

    df.sort_index(inplace=True)

    df.to_json(working_folder + "/stage_2.json")

    return 0