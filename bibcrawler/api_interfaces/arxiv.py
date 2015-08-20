#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, division
import os
import sys

import gc

import time
import datetime

import numpy as np
import pandas as pd

from rpy2.robjects.packages import SignatureTranslatedAnonymousPackage
import rpy2.robjects as R
from rpy2.robjects import pandas2ri

pandas2ri.activate()

import logging
import logging.config
from logging_dict import logging_confdict

import json

import configparser

Config = configparser.ConfigParser()
Config.read('../../config.ini')
base_directory = Config.get('directories', 'base')

from utils import get_subcat_fullname, clean_dataset, regex_old_arxiv, regex_new_arxiv


def arxiv_crawl(crawling_list, limit=None, batchsize=100, submission_range=None, update_range=None, delay=None):
    """
    This is a python wrapper for the aRxiv "arxiv_search" function.

    If submission_range or update_range are given, the results are filtered according to the date ranges.

    :param crawling_list: The subcategories to crawl. NOT "stat" -> USE "stat.AP" etc...
    :type crawling_list: dict of lists.
    :param limit: Max number of results to return.
    :type limit: int.
    :param batchsize: Number of queries per request.
    :type batchsize: int.
    :param submission_range: The range of submission dates.
    :type submission_range: Tuple (start,end).
    :param update_range: The range of last-update dates.
    :type update_range: Tuple (start,end).

    :returns:  The created folder
    """

    # Timestamp of starting datetime
    ts_start = time.time()
    timestamp = datetime.datetime.fromtimestamp(ts_start).strftime('%Y-%m-%d_%H-%M-%S')

    # Create folder structure
    working_folder = base_directory + timestamp
    os.makedirs(working_folder)
    os.makedirs(working_folder + "/temp_files")

    # Setup logging
    config = logging_confdict(working_folder, __name__)
    logging.config.dictConfig(config)
    arxiv_logger = logging.getLogger(__name__)

    arxiv_logger.info("Starting new crawl for {}".format(str(crawling_list)))
    arxiv_logger.info("Created new folder: <<" + working_folder + ">>")

    # Load R-scripts
    arxiv_logger.debug("Loading R-Scripts ...")
    try:
        with open('../r_scripts/arxiv.R', 'r') as f:
            string = ''.join(f.readlines())
        arxiv_crawler = SignatureTranslatedAnonymousPackage(string, "arxiv_crawler")
    except Exception, e:
        arxiv_logger.exception("Error while loading R-Scripts.")
        sys.exit('Could not load R-Scripts!')

    # arxiv_delay
    if delay:
        arxiv_crawler.set_delay(delay)

    # Create crawling summary for each sub-cat
    crawling_summary = pd.DataFrame(columns=["Cat.Abb", "Entries on arxiv.org", "Entries found", "Time", "Full Name"])

    temp_count = 0
    init_batchsize = batchsize
    for cat, subcats in crawling_list.iteritems():
        arxiv_logger.info("Crawling " + cat)
        subcat_len = len(subcats)
        for subcat_count, subcategory in enumerate(subcats, start=1):
            arxiv_logger.debug(subcategory)
            crawl_start = time.time()
            cat_count = arxiv_crawler.get_cat_count(subcategory)[0]

            batchsize = init_batchsize

            if not limit:
                limit = batchsize

            if limit < batchsize:
                batchsize = limit

            start_range = range(0, cat_count, batchsize) + [cat_count]

            subcat_df = pd.DataFrame()
            max_count = cat_count // batchsize
            for count, start in enumerate(start_range):
                if count == len(start_range) - 1:
                    break
                arxiv_logger.info(
                    "{}/{} {}: Batch {:<2} out of {} - start:{:<5} | batchsize:{}".format(subcat_count, subcat_len,
                                                                                          subcategory, count + 1,
                                                                                          len(start_range) - 1,
                                                                                          start,
                                                                                          start_range[count + 1] -
                                                                                          start_range[count]))
                try_count = 0
                while True:
                    try:
                        batchsize = start_range[count + 1] - start_range[count]

                        if submission_range and not update_range:
                            batch = arxiv_crawler.search_arxiv_submission_range(subcategory, limit=batchsize,
                                                                                batchsize=batchsize,
                                                                                submittedDateStart=submission_range[0],
                                                                                submittedDateEnd=submission_range[1],
                                                                                start=start)

                        elif update_range and not submission_range:
                            batch = arxiv_crawler.search_arxiv_update_range(subcategory, limit=batchsize,
                                                                            batchsize=batchsize,
                                                                            updatedStart=update_range[0],
                                                                            updatedEnd=update_range[1],
                                                                            start=start)

                        elif submission_range and update_range:
                            batch = arxiv_crawler.search_arxiv_submission_update_range(subcategory, limit=batchsize,
                                                                                       batchsize=batchsize,
                                                                                       submittedDateStart=
                                                                                       submission_range[
                                                                                           0],
                                                                                       submittedDateEnd=
                                                                                       submission_range[
                                                                                           1],
                                                                                       updatedStart=update_range[0],
                                                                                       updatedEnd=update_range[1],
                                                                                       start=start)

                        else:
                            batch = arxiv_crawler.search_arxiv(subcategory, limit=batchsize, batchsize=batchsize,
                                                               start=start)
                    except Exception, e:
                        try_count += 1
                        if 1 <= batchsize < 30:
                            try_limit = 2
                        elif 30 <= batchsize < 200:
                            try_limit = 3
                        elif 200 <= batchsize < 400:
                            try_limit = 4
                        else:
                            try_limit = 5

                        if try_count >= try_limit:
                            if batchsize == 1:
                                arxiv_logger.error("This one arxiv file is shit.".format(subcategory, try_count))
                                break
                            batchsize //= 2
                            start_range.insert(count + 1, start_range[count] + batchsize)
                            arxiv_logger.exception(
                                "{}: R-Script - Retry {} - new batchsize {}".format(subcategory, try_count, batchsize),
                                exc_info=False)
                        else:
                            arxiv_logger.exception("{}: R-Script - Retry {}".format(subcategory, try_count),
                                                   exc_info=False)
                        continue

                    else:
                        batch = pandas2ri.ri2py(batch)
                        batch_length = len(batch.index)

                        if batch_length != batchsize:
                            if count != max_count:
                                try_count += 1
                                arxiv_logger.error(
                                    "{}: Missing data from arxiv.com - Retry {}".format(subcategory, try_count))
                                continue

                        subcat_df = pd.concat([subcat_df, batch])
                        break

            crawl_end = time.time()
            result_length = len(subcat_df.index)
            crawling_summary.loc[len(crawling_summary.index) + 1] = [unicode(subcategory),
                                                                     unicode(cat_count),
                                                                     unicode(result_length),
                                                                     unicode(crawl_end - crawl_start),
                                                                     get_subcat_fullname(subcategory)]

            subcat_df = subcat_df.replace("", np.nan, regex=True)
            subcat_df.index = range(0, len(subcat_df.index))
            subcat_df["crawl_cat"] = pd.Series([subcategory] * len(subcat_df.index))

            subcat_df.to_json(working_folder + "/temp_files/temp_{}.json".format(temp_count))

            # Force garbage collection in python and R
            R.r('gc()')
            gc.collect()

            temp_count += 1

    ts_finish = time.time()

    crawling_summary.to_csv(working_folder + "/arxiv_crawl_summary.csv",
                            Config.get("csv", "sep_char"), index=False)

    arxiv_logger.info("Total crawl time: " + str(ts_finish - ts_start) + "s\n")

    # Merge all temporary files
    try:
        temp_json = {}
        for i in range(0, temp_count):
            arxiv_logger.debug(working_folder + "/temp_files/temp_{}.json".format(i))
            with open(working_folder + "/temp_files/temp_{}.json".format(i)) as data_file:
                temp = json.load(data_file)
            temp_json = {key: value for (key, value) in (temp_json.items() + temp.items())}

        result_df = pd.DataFrame.from_dict(temp_json)

        result_df.index = range(0, len(result_df.index))
        result_df = result_df.fillna(np.nan)

        result_df.to_json(working_folder + "/stage_1_raw.json")
    except:
        arxiv_logger.exception("Error during concatenation of temporary objects")

    # TODO decide if removal of temporary files is needed/useful
    # # Remove temp files
    # for i in range(0, temp_count):
    # os.remove(working_folder + "/temp_{}.json".format(i))

    return working_folder


def arxiv_cleanup(working_folder, earliest_date=None, latest_date=None,
                  remove_columns=None):
    """
    Cleans the crawl results from arxiv.

    :param working_folder: Folder containing the files
    :type working_folder: str
    :param remove_columns: Columns to be removed from the crawled dataframe. If none given, default is to remove
                           [u'abstract', u'affiliations',u'link_abstract', u'link_doi', u'link_pdf',u'comment']
    :type remove_columns: list of str
    :param earliest_date: Articles before this date are removed
    :type earliest_date: datetime
    :param latest_date: Artivles after this date are removed
    :type latest_date: datetime

    :return: None
    """

    config = logging_confdict(working_folder, __name__ + "_cleanup")
    logging.config.dictConfig(config)
    arxiv_logger = logging.getLogger(__name__ + "_cleanup")

    # Read in stage_1 raw file
    try:
        stage_1_raw = pd.read_json(working_folder + "/stage_1_raw.json")
    except Exception, e:
        arxiv_logger.exception("Could not load stage_1_raw file. Exiting...")
        sys.exit("Could not load stage_1_raw file")
    else:
        arxiv_logger.info("Stage_1_raw successfully loaded")

    if not remove_columns:
        remove_columns = eval(Config.get('data_settings', 'remove_cols'))
    stage_1 = clean_dataset(stage_1_raw, arxiv_logger, earliest_date, latest_date, remove_columns)

    stage_1['submitted'] = pd.to_datetime(stage_1['submitted'], unit="ms")
    arxiv_ids = []
    for original_arxiv in stage_1['id'].values:
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
    stage_1['arxiv_id'] = pd.Series(arxiv_ids, index=stage_1.index)

    try:
        stage_1.to_json(working_folder + "/stage_1.json")
        stage_1.to_csv(working_folder + "/stage_1.csv", encoding="utf-8",
                       sep=Config.get("csv", "sep_char"), index=False)
    except Exception, e:
        arxiv_logger.exception("Could not write all output files")
    else:
        arxiv_logger.info("Wrote json and csv output files")


def test_merge(timestamp):
    """
    Call manually if automatic merging of json files fails.

    :param timestamp: The timestamp of the crawl process that failed to merge the temporary json
    :return: <str> - Working folder
    """

    working_folder = base_directory + timestamp
    config = logging_confdict(working_folder, __name__)
    logging.config.dictConfig(config)
    arxiv_logger = logging.getLogger(__name__)

    from path import Path

    temp_files = list(Path(working_folder + "/temp_files/").files("*.json"))

    try:
        temp_jsons = []

        for idx, temp_file in enumerate(temp_files):
            arxiv_logger.debug(temp_file)
            with open(temp_file) as data_file:
                temp = json.load(data_file)
            temp_jsons.append(temp)

        temp_json = temp_jsons[0]
        for d in temp_jsons[1:-1]:
            for key, val_dict in d.items():
                new_dict = {}
                offset = len(temp_json[key].values())
                for doc_id in val_dict.keys():
                    new_doc_id = offset + int(doc_id)
                    new_dict[new_doc_id] = val_dict.pop(doc_id)
                temp_json[key].update(new_dict)
            print("Length of concatenated dataset: ", len(temp_json['id'].keys()))

        result_df = pd.DataFrame.from_dict(temp_json)

        result_df.index = range(0, len(result_df.index))
        result_df = result_df.fillna(np.nan)

        result_df.to_json(working_folder + "/stage_1_raw.json")
    except:
        arxiv_logger.exception("Error during concatenation of temporary objects")

    return working_folder
