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

from config import base_directory

__author__ = 'Asura Enkhbayar <asura.enkhbayar@gmail.com>'

#: Levenshtein Ratio - Schloegl et al, 2014
LR = 1 / 15.83

# Regex for 'only alpha-numeric'
regex_alphanum = r"[^a-zA-Z0-9]"
regex_mult_whitespace = r"\s{2,}"


def crossref_lookup(authors, titles, submitted):
    # Load r-scripts
    with open('r_scripts/doi_lookup.R', 'r') as f:
        string = ''.join(f.readlines())
    doi_lookuper = SignatureTranslatedAnonymousPackage(string, "doi_lookuper")

    cr_lookup = None
    for author, title, date in zip(authors, titles, submitted):
        if cr_lookup is None:
            cr_lookup = com.convert_robj(doi_lookuper.crossref(author, title, date))
        else:
            cr_lookup = pd.concat([cr_lookup, com.convert_robj(doi_lookuper.crossref(author, title, date))])

    cr_dois = []
    levenshtein_ratio = []
    for original, found, doi in zip(titles, cr_lookup.title, cr_lookup.DOI):
        original = re.sub(regex_alphanum, " ", original).strip()
        original = re.sub(regex_mult_whitespace, " ", original).lower()

        found = re.sub(regex_alphanum, " ", found).strip()
        found = re.sub(regex_mult_whitespace, " ", found).lower()

        ld = distance(unicode(original), unicode(found))
        max_len = max(len(original), len(found))

        if ld / max_len <= LR:
            cr_dois.append(doi)
        else:
            cr_dois.append(None)

        levenshtein_ratio.append(ld / max_len)

    return cr_dois, levenshtein_ratio


def doi_lookup(stage1_dir = None, mode='all'):
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
        all_subdirs = [base_directory+d for d in os.listdir(base_directory) if os.path.isdir(base_directory+d)]
        latest_subdir = max(all_subdirs, key=os.path.getmtime)
        stage1_dir = latest_subdir + "/"

    working_folder = stage1_dir + timestamp
    if not os.path.exists(working_folder):
        os.makedirs(working_folder)
    else:
        print("The crawl <<" + working_folder + ">> already exists. Exiting...")
        return None

    # Read in stage 1 file
    df = pd.io.json.read_json(stage1_dir+"stage_1.json")

    # Crawl additional dois
    cr_dois = []
    levenshtein_ratio = []

    if mode == 'all':
        cr_dois, levenshtein_ratio = crossref_lookup(df.authors, df.title, df.submitted)

    elif mode == 'crossref':
        cr_dois = crossref_lookup(df.authors, df.titles, df.submitted)

    elif mode == 'datacite':
        pass

    df['crossref_doi'] = pd.Series(cr_dois)
    df['levenshtein_ratio'] = pd.Series(levenshtein_ratio)

    df.to_json(working_folder + "/stage_2.json")

    return 0