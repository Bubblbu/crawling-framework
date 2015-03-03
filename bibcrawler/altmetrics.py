#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, division
from pprint import pprint

import os
import time
import datetime

import numpy as np
import pandas as pd

from config import base_directory, mndly_config

from mendeley import Mendeley
from mendeley.exception import MendeleyException, MendeleyApiException


def start_mendeley_session(mndly_config):
    mendeley = Mendeley(mndly_config['client_id'], mndly_config['secret'])
    auth = mendeley.start_client_credentials_flow()
    return auth.authenticate()


def mendeley_altmetrics(stage1_dir=None, stage2_dir=None):
    ts_start = time.time()
    timestamp = datetime.datetime.fromtimestamp(ts_start).strftime('%Y-%m-%d_%H-%M-%S')

    # Start mendeley session
    session = start_mendeley_session(mndly_config)

    # Create folder structure
    if not stage1_dir:
        all_subdirs = [base_directory + d for d in os.listdir(base_directory) if os.path.isdir(base_directory + d)]
        latest_subdir = max(all_subdirs, key=os.path.getmtime)
        stage1_dir = latest_subdir + "/"

    print(stage1_dir)

    if not stage2_dir:
        all_subdirs = [stage1_dir + d for d in os.listdir(stage1_dir) if os.path.isdir(stage1_dir + d)]
        latest_subdir = max(all_subdirs, key=os.path.getmtime)
        stage2_dir = latest_subdir + "/"

    working_folder = stage2_dir + timestamp
    if not os.path.exists(working_folder):
        os.makedirs(working_folder)
    else:
        print("The crawl <<" + working_folder + ">> already exists. Exiting...")
        return None

    # Read in stage 2 file
    df = pd.io.json.read_json(stage2_dir + "stage_2.json")

    for idx, row in df.iterrows():
        if row['levenshtein_ratio'] < 1 / 15.83:
            print(row['id'], " | ", row['title'], " | ", row['crossref_title'], "|", row['levenshtein_ratio'])


if __name__ == "__main__":
    mendeley_altmetrics()

