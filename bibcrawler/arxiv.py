#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, division

import os
import gc
import time
import datetime
import numpy as np
import pandas as pd
import logging
import logging.config
from logging_dict import logging_confdict

from rpy2.robjects.packages import SignatureTranslatedAnonymousPackage
import rpy2.robjects as R
import pandas.rpy.common as com

from config import base_directory
from utils import get_subcat_fullname

__author__ = 'Asura Enkhbayar <asura.enkhbayar@gmail.com>'


def r_arxiv_crawler(crawling_list, limit=None, batchsize=100, submission_range=None, update_range=None, delay=None):
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

    :returns:  pd.DataFrame -- the resulting data frame.
    """

    # Timestamp of starting datetime
    ts_start = time.time()
    timestamp = datetime.datetime.fromtimestamp(ts_start).strftime('%Y-%m-%d_%H-%M-%S')

    # Create folder structure
    working_folder = base_directory + timestamp
    os.makedirs(working_folder)

    # Setup logging
    config = logging_confdict(working_folder, __name__)
    logging.config.dictConfig(config)
    arxiv_logger = logging.getLogger(__name__)

    arxiv_logger.info("Starting new crawl for {}".format(str(crawling_list)))
    arxiv_logger.info("Created new folder: <<" + working_folder + ">>")

    # Load R-scripts
    arxiv_logger.debug("Loading R-Scripts ...")
    try:
        with open('r_scripts/arxiv.R', 'r') as f:
            string = ''.join(f.readlines())
        arxiv_crawler = SignatureTranslatedAnonymousPackage(string, "arxiv_crawler")
    except Exception, e:
        arxiv_logger.exception("Error while loading R-Scripts.")

    # arxiv_delay
    if delay:
        arxiv_crawler.set_delay(delay)

    # Crawling
    crawl_log = pd.DataFrame(columns=["Cat.Abb", "Entries on arxiv.org", "Entries found", "Time", "Full Name"])

    temp_count = 0
    for cat, subcats in crawling_list.iteritems():
        arxiv_logger.info("Crawling " + cat)
        for subcategory in subcats:
            arxiv_logger.debug(subcategory)
            crawl_start = time.time()
            cat_count = arxiv_crawler.get_cat_count(subcategory)[0]

            start_range = range(0, cat_count, batchsize)

            if not limit:
                limit = batchsize

            if limit < batchsize:
                batchsize = limit

            subcat_df = pd.DataFrame()
            max_count = cat_count // batchsize
            for count, start in enumerate(start_range):
                arxiv_logger.info("{}: Batch {} out of {} - start:{}|bs:{}".format(subcategory, count, len(start_range)-1,
                try_count = 0
                while True:
                    try:
                        if submission_range and not update_range:
                            batch = arxiv_crawler.search_arxiv_submission_range(subcategory, limit=limit,
                                                                                batchsize=batchsize,
                                                                                submittedDateStart=submission_range[0],
                                                                                submittedDateEnd=submission_range[1],
                                                                                start=start)

                        elif update_range and not submission_range:
                            batch = arxiv_crawler.search_arxiv_update_range(subcategory, limit=limit,
                                                                            batchsize=batchsize,
                                                                            updatedStart=update_range[0],
                                                                            updatedEnd=update_range[1],
                                                                            start=start)

                        elif submission_range and update_range:
                            batch = arxiv_crawler.search_arxiv_submission_update_range(subcategory, limit=limit,
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
                            batch = arxiv_crawler.search_arxiv(subcategory, limit=limit, batchsize=batchsize,
                                                               start=start)
                    except Exception, e:
                        try_count += 1
                        print("\t\t\t SOME ERROR OCCURED... Retry {}".format(try_count))

                        # TODO EXCEPTION HANDLING
                        continue

                    else:
                        batch = com.convert_robj(batch)
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
            crawl_log.loc[len(crawl_log.index) + 1] = [unicode(subcategory),
                                                       unicode(cat_count),
                                                       unicode(result_length),
                                                       unicode(crawl_end - crawl_start),
                                                       get_subcat_fullname(subcategory)]

            # TODO: Save temporary files to HDD. After crawling all of them concatenate to one file. Remove temp files
            # result_df = pd.concat([result_df, subcat_df])

            subcat_df = subcat_df.replace("", np.nan, regex=True)
            subcat_df.index = range(0, len(subcat_df.index))
            subcat_df.to_json(working_folder + "/temp_{}.json".format(temp_count))

            # Force garbage collection in python and R
            R.r('gc()')
            gc.collect()

            temp_count += 1

    ts_finish = time.time()

    # Create log files

    crawl_log.to_csv(working_folder + "/crawl_log.csv", sep=";")
    write_log(working_folder, ts_start, ts_finish)

    arxiv_logger.info("Total crawl time: " + str(ts_finish - ts_start) + "s\n")

    # Merge all temporary files
    temp_dfs = []
    try:
        for i in range(0, temp_count):
            arxiv_logger.debug(working_folder + "/temp_{}.json".format(i))
            temp_dfs.append(pd.io.json.read_json(working_folder + "/temp_{}.json".format(i)))
        result_df = pd.concat(temp_dfs)

        result_df.index = range(0, len(result_df.index))
        result_df.to_json(working_folder + "/stage_1.json")
    except Exception, e:
        arxiv_logger.exception("Error during concatenation of temporary objects")

    # # Remove temp files
    # for i in range(0, temp_count):
    #     os.remove(working_folder + "/temp_{}.json".format(i))

    return


def write_log(directory, start_time, end_time):
    with open(directory + "/log.txt", "wb") as outfile:
        outfile.write("--- LOG --- " + datetime.datetime.fromtimestamp(start_time).strftime('%Y-%m-%d_%H-%M-%S') + "\n\n")

        outfile.write("Total crawl time: " + unicode(end_time - start_time) + "s\n")

        outfile.write("Have a look at log.csv for more details on the crawl.\n\n")

        outfile.write("TO-DO: Notes and other logging stuff")