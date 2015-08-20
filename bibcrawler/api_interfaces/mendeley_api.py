#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, division
import sys

import os
import time
import datetime
import threading
import Queue

import numpy as np
import pandas as pd

import logging
import logging.config
from logging_dict import logging_confdict

import configparser

Config = configparser.ConfigParser()
Config.read('../../config.ini')
base_directory = Config.get('directories', 'base')

from utils import regex_old_arxiv, regex_new_arxiv, regex_doi, levenshtein_ratio, LR, clean_dataset

from mendeley import Mendeley
from mendeley.exception import MendeleyException, MendeleyApiException


class MendeleyThread(threading.Thread):
    """
    Mendeley Thread.
    Takes single rows from stage_2 as input and returns stage_3 rows as dicts.
    """

    def __init__(self, logger, input_q, output_q, max_len, session):
        threading.Thread.__init__(self)
        self.logger = logger
        self.input_q = input_q
        self.output_q = output_q
        self.max_len = max_len
        self.session = session

    def run(self):
        while not self.input_q.empty():
            count, row = self.input_q.get_nowait()
            count = int(count)

            arxiv_document = None
            doi_document = None

            arxiv_id = row['id']
            found_regex = regex_new_arxiv.findall(arxiv_id)
            if found_regex:
                arxiv_id = found_regex[0]
            else:
                found_regex = regex_old_arxiv.findall(arxiv_id)
                if found_regex:
                    arxiv_id = found_regex[0]

            if regex_doi.match(str(row['doi'])):
                arxiv_doi = row['doi']
            else:
                if regex_doi.match(str(row['cr_doi'])):
                    arxiv_doi = row['cr_doi']
                else:
                    arxiv_doi = None

            self.logger.debug("\n=== FILE {} of {} === id:{}".format(count, self.max_len, arxiv_id))

            src = {'arxiv_id': arxiv_id,
                   'title': row['title'],
                   'mndly_path': np.nan}

            if arxiv_doi:
                try:
                    doi_document = self.session.catalog.by_identifier(doi=arxiv_doi, view='all')
                except (MendeleyException, MendeleyApiException), e:
                    pass
                except Exception, e:
                    self.logger.exception("File:{} - ID:{}: Some other error occured".format(count, arxiv_id))

                try:
                    arxiv_document = self.session.catalog.by_identifier(arxiv=arxiv_id, view='all')
                except (MendeleyException, MendeleyApiException), e:
                    pass
                except Exception, e:
                    self.logger.exception("File {} - {}: Some other error occured".format(count, arxiv_id))

                if doi_document and not arxiv_document:
                    self.logger.info("File {} - {}: DOI doc found - No arxiv doc".format(count, arxiv_id))
                    src['mndly_path'] = 1
                    self.output_q.put(add_new_entry(src, doi_document))

                elif not doi_document and arxiv_document:
                    self.logger.info("File {} - {}: No DOI doc - Arxiv doc found".format(count, arxiv_id))
                    src['mndly_path'] = 2
                    self.output_q.put(add_new_entry(src, arxiv_document))

                elif not doi_document and not arxiv_document:
                    self.logger.info("File {} - {}: No DOI doc - No arxiv doc".format(count, arxiv_id))
                    src['mndly_path'] = 7
                    self.output_q.put(add_new_entry(src, None))

                else:
                    if doi_document.id == arxiv_document.id:
                        self.logger.info("File {} - {}: Found both DOI/Arxiv docs - identical".format(count, arxiv_id))
                        src['mndly_path'] = 3
                        self.output_q.put(add_new_entry(src, arxiv_document))
                    else:
                        if len(doi_document.identifiers) >= len(arxiv_document.identifiers):
                            self.logger.info(
                                "File {} - {}: Found both DOI/Arxiv docs - DOI choosen".format(count, arxiv_id))
                            src['mndly_path'] = 4
                            self.output_q.put(add_new_entry(src, doi_document))
                        else:
                            self.logger.info(
                                "File {} - {}: Found both DOI/Arxiv docs - Arxiv chosen".format(count, arxiv_id))
                            src['mndly_path'] = 5
                            self.output_q.put(add_new_entry(src, arxiv_document))
            else:
                try:
                    arxiv_document = self.session.catalog.by_identifier(arxiv=arxiv_id, view='all')
                    if arxiv_document:
                        self.logger.info("File {} - {}: Arxiv doc found".format(count, arxiv_id))
                        src['mndly_path'] = 6
                        self.output_q.put(add_new_entry(src, arxiv_document))
                except (MendeleyException, MendeleyApiException), e:
                    self.logger.info("File {} - {}: No arxiv doc found".format(count, arxiv_id))
                    src['mndly_path'] = 8
                    self.output_q.put(add_new_entry(src, None))
                except Exception, e:
                    self.logger.exception("File {} - {}: Some other error occured".format(count, arxiv_id))


def start_mendeley_session(mndly_config):
    """
    Creates a new Mendeley session
    :param mndly_config: Contains information 'client_id', 'secret'
    :return: session
    """
    mendeley = Mendeley(mndly_config['client_id'], mndly_config['secret'])
    auth = mendeley.start_client_credentials_flow()
    return auth.authenticate()


def init_temp(src):
    """
    Initialise output document
    """
    temp = dict()
    temp['arxiv_id'] = src['arxiv_id']
    temp['mndly_path'] = src['mndly_path']

    temp['mndly_match'] = np.nan
    temp['type'] = np.nan
    temp['identifiers'] = np.nan
    temp['keywords'] = np.nan
    temp['abstract'] = np.nan
    temp['link'] = np.nan
    temp['mndly_authors'] = np.nan

    # bibliometic data
    temp['pages'] = np.nan
    temp['volume'] = np.nan
    temp['issue'] = np.nan
    temp['websites'] = np.nan
    temp['month'] = np.nan
    temp['publisher'] = np.nan
    temp['day'] = np.nan
    temp['city'] = np.nan
    temp['edition'] = np.nan
    temp['institution'] = np.nan
    temp['series'] = np.nan
    temp['chapter'] = np.nan
    temp['revision'] = np.nan
    temp['editors'] = np.nan

    # readership statistics
    temp['reader_count'] = np.nan
    temp['reader_count_by_academic_status'] = np.nan
    temp['reader_count_by_subdiscipline'] = np.nan
    temp['reader_count_by_country'] = np.nan

    return temp


def add_new_entry(src, mndly_doc):
    """
    Creates the entry for the stage_3 output from src document and corresponding mendeley document

    :param src: The stage_2 source document.
                Used keys are 'arxiv_id','arxiv_doi':,'cr_doi','title','submitted','mndly_path'
    :type src: dict
    :param mndly_doc: The found mendeley document
    :type mndly_doc: CatalogDocument <mendeley.models.catalog.CatalogDocument>
    :return: Merged document. :class: dict
    """
    temp = init_temp(src)

    # Check with arXiv data
    if not mndly_doc:
        pass
    else:
        if 'arxiv' in mndly_doc.identifiers:
            arxiv_id = src['arxiv_id']
            mndly_arxiv_id = mndly_doc.identifiers['arxiv']

            # TODO - remove the version number
            found_regex = regex_new_arxiv.findall(mndly_doc.identifiers['arxiv'])
            if found_regex:
                mndly_arxiv_id = found_regex[0]
            else:
                found_regex = regex_old_arxiv.findall(mndly_doc.identifiers['arxiv'])
                if found_regex:
                    mndly_arxiv_id = found_regex[0]

            found_regex = regex_new_arxiv.findall(src['arxiv_id'])
            if found_regex:
                arxiv_id = found_regex[0]
            else:
                found_regex = regex_old_arxiv.findall(mndly_doc.identifiers['arxiv'])
                if found_regex:
                    arxiv_id = found_regex[0]

            if arxiv_id == mndly_arxiv_id:
                check = True
            else:
                check = False
        else:
            lr = levenshtein_ratio(src['title'], mndly_doc.title)
            if lr <= LR:
                check = True
            else:
                check = False

        if check:
            temp['mndly_match'] = True
        else:
            temp['mndly_match'] = False

        temp['type'] = mndly_doc.type
        temp['identifier_count'] = len(mndly_doc.identifiers)
        temp['identifiers'] = "-".join(sorted(mndly_doc.identifiers))

        temp['mndly_arxiv'] = mndly_doc.identifiers['arxiv'] if 'arxiv' in mndly_doc.identifiers else np.nan
        temp['mndly_doi'] = mndly_doc.identifiers['doi'] if 'doi' in mndly_doc.identifiers else np.nan
        temp['mndly_issn'] = mndly_doc.identifiers['issn'] if 'issn' in mndly_doc.identifiers else np.nan
        temp['mndly_isbn'] = mndly_doc.identifiers['isbn'] if 'isbn' in mndly_doc.identifiers else np.nan
        temp['mndly_scopus'] = mndly_doc.identifiers['scopus'] if 'scopus' in mndly_doc.identifiers else np.nan
        temp['mndly_pmid'] = mndly_doc.identifiers['pmid'] if 'pmid' in mndly_doc.identifiers else np.nan

        temp['keywords'] = mndly_doc.keywords
        temp['abstract'] = mndly_doc.abstract
        temp['link'] = mndly_doc.link
        try:
            temp['mndly_authors'] = [{'first_name': elem.first_name, 'last_name': elem.last_name} for elem in
                                     mndly_doc.authors]
        except TypeError:
            temp['mndly_authors'] = []

        # bibliometic data
        temp['pages'] = mndly_doc.pages
        temp['volume'] = mndly_doc.volume
        temp['issue'] = mndly_doc.issue
        temp['websites'] = mndly_doc.websites
        temp['month'] = mndly_doc.month
        temp['publisher'] = mndly_doc.publisher
        temp['day'] = mndly_doc.day
        temp['city'] = mndly_doc.city
        temp['edition'] = mndly_doc.edition
        temp['institution'] = mndly_doc.institution
        temp['series'] = mndly_doc.series
        temp['chapter'] = mndly_doc.chapter
        temp['revision'] = mndly_doc.revision
        try:
            temp['editors'] = [{'first_name': elem.first_name, 'last_name': elem.last_name} for elem in
                               mndly_doc.editors]
        except TypeError:
            temp['editors'] = []

        # readership statistics
        temp['reader_count'] = mndly_doc.reader_count
        temp['reader_count_by_academic_status'] = mndly_doc.reader_count_by_academic_status
        temp['reader_count_by_subdiscipline'] = mndly_doc.reader_count_by_subdiscipline
        temp['reader_count_by_country'] = mndly_doc.reader_count_by_country
        # else:
        # if 'arxiv' in mndly_doc.identifiers:
        # ret_val = (src['arxiv_id'], mndly_doc.identifiers['arxiv'])
        # else:
        #         ret_val = (unicode(src['title']).encode('utf-8'), unicode(mndly_doc.title).encode('utf-8'))
        #
        #     temp['mndly_kicked'] = ret_val

    return temp


def mendeley_crawl(stage1_dir=None, stage2_dir=None, num_threads=1):
    """
    Retrieve mendeley documents based on arxiv id and dois.
    If both arxiv and doi yield different mendeley documents the one with more identifiers is preferred.

    :param stage1_dir: The name of the Stage 1 folder to use. If None last created will be used
    :param stage2_dir: The name of the Stage 2 folder to use. If None last created will be used
    :param num_threads: Number of threads to use
    :return: working_folder as absolute path
    """

    ts_start = time.time()
    timestamp = datetime.datetime.fromtimestamp(ts_start).strftime('%Y-%m-%d_%H-%M-%S')

    # Start mendeley session
    session = start_mendeley_session(Config._sections['mndly_auth'])

    # Create folder structure
    if not stage1_dir:
        all_subdirs = [base_directory + d for d in os.listdir(base_directory) if os.path.isdir(base_directory + d)]
        latest_subdir = max(all_subdirs, key=os.path.getmtime)
        stage1_dir = latest_subdir + "/"
    else:
        stage1_dir += "/"

    if not stage2_dir:
        all_subdirs = [stage1_dir + d for d in os.listdir(stage1_dir) if os.path.isdir(stage1_dir + d)]
        latest_subdir = max(all_subdirs, key=os.path.getmtime)
        stage2_dir = latest_subdir + "/"
    else:
        stage2_dir = stage1_dir + stage2_dir + "/"

    working_folder = stage2_dir + timestamp
    if not os.path.exists(working_folder):
        os.makedirs(working_folder)

    # Create logger
    config = logging_confdict(working_folder, __name__)
    logging.config.dictConfig(config)
    logger = logging.getLogger(__name__)

    # Read in stage 2 file
    input_df = pd.read_json(stage2_dir + "stage_2.json")
    input_df.sort_index(inplace=True)

    input_q = Queue.Queue()
    output_q = Queue.Queue()

    for idx, row in input_df.iterrows():
        input_q.put((idx, row))

    mndly_threads = []
    for i in range(0, num_threads):
        thread = MendeleyThread(logger, input_q, output_q, len(input_df.index), session)
        thread.start()
        mndly_threads.append(thread)

    for thread in mndly_threads:
        thread.join()

    output_dicts = []
    while not output_q.empty():
        output_dicts.append(output_q.get_nowait())

    # ================= TEMPORARY HACK ==============
    arxiv_ids = []
    for original_arxiv in input_df['id'].values:
        found_regex = regex_new_arxiv.findall(original_arxiv)
        if found_regex:
            arxiv_id = found_regex[0]
        else:
            found_regex = regex_old_arxiv.findall(original_arxiv)
            if found_regex:
                arxiv_id = found_regex[0]
            else:
                arxiv_id = "parse_failed"
        arxiv_ids.append(arxiv_id)
    input_df['arxiv_id'] = pd.Series(arxiv_ids, index=input_df.index)
    #  ================= TEMPORARY HACK ==============

    stage_3_raw = pd.DataFrame(output_dicts)
    stage_3_raw = pd.merge(left=input_df,
                           right=stage_3_raw,
                           left_on="arxiv_id",
                           right_on="arxiv_id",
                           how="outer")

    stage_3_raw['submitted'] = pd.to_datetime(stage_3_raw['submitted'], unit="ms")
    stage_3_raw['updated'] = pd.to_datetime(stage_3_raw['updated'], unit="ms")

    del stage_3_raw['abstract']

    try:
        stage_3_raw.to_json(working_folder + "/stage_3_raw.json")
        stage_3_raw.to_csv(working_folder + "/stage_3_raw.csv", encoding="utf-8",
                           sep=Config.get("csv", "sep_char"), index=False)
    except Exception, e:
        logger.exception("Could not write all output files")
    else:
        logger.info("Wrote stage_3_raw json and csv output files")

    return working_folder


def mendeley_cleanup(working_folder, earliest_date=None, latest_date=None,
                     remove_columns=None):
    """
    Cleans the crawl results from mendeley.

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
        stage_3_raw = pd.read_json(working_folder + "/stage_3_raw.json")
    except Exception, e:
        cr_logger.exception("Could not load stage_1_raw file")
        sys.exit("Could not load stage 2 raw")
    else:
        cr_logger.info("Stage_1_raw successfully loaded")

    if not remove_columns:
        remove_columns = eval(Config.get('data_settings', 'remove_cols'))
    stage_3 = clean_dataset(stage_3_raw, cr_logger, earliest_date, latest_date, remove_columns)

    stage_3.index = range(0, len(stage_3.index))

    try:
        stage_3.to_json(working_folder + "/stage_3.json")
        stage_3.to_csv(working_folder + "/stage_3.csv", encoding="utf-8",
                       sep=Config.get("csv", "sep_char"), index=False)

    except Exception, e:
        cr_logger.exception("Could not write all output files")
    else:
        cr_logger.info("Wrote stage-3 cleaned json and csv output files")
