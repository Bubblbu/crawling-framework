#!/usr/bin/python
# -*- coding: utf-8 -*-

from __future__ import print_function, division

import os

import time
import datetime

import configparser

Config = configparser.ConfigParser()
Config.read('../../config.ini')
base_directory = Config.get('directories', 'base')


def ads_crawl(input_folder=None):
    """

    :param input_folder: Input folder
    :return: Newly created working folder
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

    #TODO

    return working_folder