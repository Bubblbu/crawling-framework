#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, division
from pprint import pprint

from Levenshtein import distance

from rpy2.robjects.packages import SignatureTranslatedAnonymousPackage
import pandas.rpy.common as com
import pandas as pd

import os
import time
import datetime
import re
import threading
import Queue

from config import base_directory

__author__ = 'Asura Enkhbayar <asura.enkhbayar@gmail.com>'

#: Levenshtein Ratio - Schloegl et al, 2014
LR = 1 / 15.83

# Regex for 'only alpha-numeric'
regex_alphanum = r"[^a-zA-Z0-9]"
regex_mult_whitespace = r"\s{2,}"


class CrossrefThread(threading.Thread):
    def __init__(self, queue, output_queue, doi_lookuper):
        threading.Thread.__init__(self)
        self.event = threading.Event()
        self.q = queue
        self.output_queue = output_queue
        self.doi_lookuper = doi_lookuper

    def run(self):
        while True:
            try:
                idx, author, title, date = self.q.get(timeout=1)
                print(idx)
                temp = com.convert_robj(self.doi_lookuper.crossref(author, title, date))
                temp['order'] = idx
                temp['orig_title'] = title
                self.output_queue.put(temp)
            except Queue.Empty:
                if self.event.is_set():
                    break


def crossref_lookup(index, authors, titles, submitted, num_threads=1):
    # Load r-scripts
    with open('r_scripts/doi_lookup.R', 'r') as f:
        string = ''.join(f.readlines())
    doi_lookuper = SignatureTranslatedAnonymousPackage(string, "doi_lookuper")

    input_queue = Queue.Queue()
    output_queue = Queue.Queue()

    cr_lookup = pd.DataFrame()
    for idx, author, title, date in zip(index, authors, titles, submitted):
        input_queue.put((idx, author, title, date))

    crossref_threads = []

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
    levenshtein_ratio = []
    cr_titles = []

    for original, found, doi in zip(cr_lookup.orig_title, cr_lookup.title, cr_lookup.DOI):
        original_edited = re.sub(regex_alphanum, " ", original).strip()
        original_edited = re.sub(regex_mult_whitespace, " ", original_edited).lower()

        found_edited = re.sub(regex_alphanum, " ", found).strip()
        found_edited = re.sub(regex_mult_whitespace, " ", found_edited).lower()

        ld = distance(unicode(original_edited), unicode(found_edited))
        max_len = max(len(original_edited), len(found_edited))

        if ld / max_len <= LR:
            cr_dois.append(doi)
            cr_titles.append(found)
        else:
            cr_dois.append(None)
            cr_titles.append(None)

        levenshtein_ratio.append(ld / max_len)

    return cr_dois, levenshtein_ratio, cr_titles


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
    else:
        stage1_dir = base_directory + stage1_dir
        if stage1_dir[-1] != "/":
            stage1_dir += "/"

    working_folder = stage1_dir + timestamp
    if not os.path.exists(working_folder):
        os.makedirs(working_folder)
    else:
        print("The crawl <<" + working_folder + ">> already exists. Exiting...")
        return None

    # Read in stage 1 file
    df = pd.io.json.read_json(stage1_dir + "stage_1.json")
    df.index = range(0, len(df.index))

    # Crawl additional dois
    cr_dois = []
    levenshtein_ratio = []
    cr_titles = []

    if mode == 'all':
        cr_dois, levenshtein_ratio, cr_titles = crossref_lookup(df.index, df.authors, df.title, df.submitted,
                                                                num_threads=num_workers)

    elif mode == 'crossref':
        cr_dois, levenshtein_ratio, cr_titles = crossref_lookup(df.index, df.authors, df.title, df.submitted,
                                                                num_threads=num_workers)

    elif mode == 'datacite':
        pass

    df['crossref_doi'] = pd.Series(cr_dois)
    df['levenshtein_ratio'] = pd.Series(levenshtein_ratio)
    df['crossref_title'] = pd.Series(cr_titles)

    df.sort_index(inplace=True)

    df.to_json(working_folder + "/stage_2.json")

    return 0