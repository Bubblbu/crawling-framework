#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, division

import os
import time
import datetime

import numpy as np
import pandas as pd

from config import base_directory, mndly_config
from utils import levenshtein_ratio, LR, doi_check, old_arxiv_format, new_arxiv_format

from mendeley import Mendeley
from mendeley.exception import MendeleyException, MendeleyApiException


def start_mendeley_session(mndly_config):
    mendeley = Mendeley(mndly_config['client_id'], mndly_config['secret'])
    auth = mendeley.start_client_credentials_flow()
    return auth.authenticate()


def add_new_entry(list_of_dicts, src, mndly_doc):
    # Check with arXiv data
    if 'arxiv' in mndly_doc.identifiers:
        arxiv_id = src['arxiv_id']
        mndly_arxiv_id = mndly_doc.identifiers['arxiv']
        found_regex = new_arxiv_format.findall(mndly_doc.identifiers['arxiv'])
        if found_regex:
            mndly_arxiv_id = found_regex[0]
        else:
            found_regex = old_arxiv_format.findall(mndly_doc.identifiers['arxiv'])
            if found_regex:
                mndly_arxiv_id = found_regex[0]

        found_regex = new_arxiv_format.findall(src['arxiv_id'])
        if found_regex:
            arxiv_id = found_regex[0]
        else:
            found_regex = old_arxiv_format.findall(mndly_doc.identifiers['arxiv'])
            if found_regex:
                arxiv_id = found_regex[0]

        if arxiv_id == mndly_arxiv_id:
            print("=== GOOD ===")
            print(arxiv_id, " - ", mndly_arxiv_id)
            print(src['title'], " - ", mndly_doc.title)
            check = True
        else:
            print("=== BAD ===")
            print(arxiv_id, " - ", mndly_arxiv_id)
            print(src['title'], " - ", mndly_doc.title)
            check = False

    else:
        lr = levenshtein_ratio(src['title'], mndly_doc.title)
        if lr <= LR:
            print("=== GOOD === {}".format(lr))
            print(src['title'], " - ", mndly_doc.title)
            check = True
        else:
            print("=== BAD === {}".format(lr))
            print(src['title'], " - ", mndly_doc.title)
            check = False

    temp = dict()
    if check:
        temp['arxiv_id'] = src['arxiv_id']
        temp['arxiv_doi'] = src['arxiv_doi']
        temp['crossref_doi'] = src['crossref_doi']
        temp['mndly_found'] = True
        temp['mndly_path'] = src['mndly_path']
        temp['mndly_kicked'] = np.nan

        temp['type'] = mndly_doc.type
        temp['identifiers'] = mndly_doc.identifiers
        temp['keywords'] = mndly_doc.keywords
        temp['abstract'] = mndly_doc.abstract
        temp['link'] = mndly_doc.link
        try:
            temp['authors'] = [{'first_name': elem.first_name, 'last_name': elem.last_name} for elem in
                               mndly_doc.authors]
        except TypeError:
            temp['authors'] = []

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

        list_of_dicts.append(temp)

        return

    else:
        print("******  FOUND MEND mndly_doc DID NOT MATCH ARXIV   *******")
        if 'arxiv' in mndly_doc.identifiers:
            print(src['arxiv_id'] + " - " + mndly_doc.identifiers['arxiv'])
            ret_val = (src['arxiv_id'], mndly_doc.identifiers['arxiv'])

        else:
            print(src['title'] + " - " + mndly_doc.title)
            ret_val = (unicode(src['title']).encode('utf-8'), unicode(mndly_doc.title).encode('utf-8'))

        temp['arxiv_id'] = src['arxiv_id']
        temp['arxiv_doi'] = src['arxiv_doi']
        temp['crossref_doi'] = src['crossref_doi']
        temp['mndly_found'] = False
        temp['mndly_path'] = src['mndly_path']
        temp['mndly_kicked'] = ret_val

        temp['type'] = np.nan
        temp['identifiers'] = np.nan
        temp['keywords'] = np.nan
        temp['abstract'] = np.nan
        temp['link'] = np.nan
        temp['authors'] = np.nan


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

        list_of_dicts.append(temp)

        return


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
    else:
        stage1_dir = base_directory + stage1_dir + "/"

    if not stage2_dir:
        all_subdirs = [stage1_dir + d for d in os.listdir(stage1_dir) if os.path.isdir(stage1_dir + d)]
        latest_subdir = max(all_subdirs, key=os.path.getmtime)
        stage2_dir = latest_subdir + "/"

    working_folder = stage2_dir + timestamp
    os.makedirs(working_folder)

    # Read in stage 2 file
    input_df = pd.io.json.read_json(stage2_dir + "stage_2.json")
    input_df = input_df.fillna(value=np.nan)
    input_df = input_df.replace("", np.nan)

    output_dicts = []

    for idx, row in input_df.iterrows():
        arxiv_id = row['id']
        if doi_check.match(str(row['doi'])):
            doi = row['doi']
        else:
            if doi_check.match(str(row['crossref_doi'])):
                doi = row['crossref_doi']
            else:
                doi = None

        src = {'arxiv_id': arxiv_id,
               'arxiv_doi': row['doi'],
               'crossref_doi': row['crossref_doi'],
               'title': row['title'],
               'mndly_path': np.nan}

        if doi:
            doi_document = None
            try:
                doi_document = session.catalog.by_identifier(doi=doi, view='all')
            except (MendeleyException, MendeleyApiException):
                pass
            except Exception, e:
                print("HTTP", str(e))

            arxiv_document = None
            try:
                arxiv_document = session.catalog.by_identifier(arxiv=arxiv_id, view='all')
            except (MendeleyException, MendeleyApiException):
                pass
            except Exception, e:
                print("HTTP", str(e))

            if doi_document and not arxiv_document:
                print("\ndoi found, but no arxiv")
                add_new_entry(output_dicts, src, doi_document)

            elif not doi_document and arxiv_document:
                print("\narxiv found, but no doi")
                add_new_entry(output_dicts, src, arxiv_document)

            elif not doi_document and not arxiv_document:
                print("\nNone found")
                continue

            else:
                print("\nboth found")
                # self.arxiv_doi_found = [id_mndly_doc.arxiv_id, True, True]
                # self.lookup_doi_and_arxiv_both += 1
                # if doi_mndly_document.id == arxiv_mndly_document.id:
                # self.path = 3
                # self.lookup_doi_and_arxiv_both_match += 1
                #     ret_val = id_mndly_doc.add_mendeley_data(doi_mndly_document)
                #     if ret_val[0]:
                #         self.mndly_docs_after_fuzzy_title_match += 1
                #         self.mndly_document_queue.put(id_mndly_doc)
                #
                #     else:
                #         self.kicked = ret_val[1]
                #         self.kicked_doi += 1
                # else:
                #     if len(doi_mndly_document.identifiers) >= len(arxiv_mndly_document.identifiers):
                #         self.path = 4
                #         self.lookup_doi_and_arxiv_both_doi += 1
                #         ret_val = id_mndly_doc.add_mendeley_data(doi_mndly_document)
                #         if ret_val[0]:
                #             self.mndly_docs_after_fuzzy_title_match += 1
                #             self.mndly_document_queue.put(id_mndly_doc)
                #         else:
                #             self.kicked = ret_val[1]
                #             self.kicked_doi += 1
                #     else:
                #         self.path = 5
                #         self.lookup_doi_and_arxiv_both_arxiv += 1
                #         ret_val = id_mndly_doc.add_mendeley_data(arxiv_mndly_document)
                #         if ret_val[0]:
                #             self.mndly_docs_after_fuzzy_title_match += 1
                #             self.mndly_document_queue.put(id_mndly_doc)
                #         else:
                #             self.kicked = ret_val[1]
                #             self.kicked_arxiv += 1
        else:
            try:
                arxiv_document = session.catalog.by_identifier(arxiv=arxiv_id, view='all')
                if arxiv_document:
                    print("arxiv found - solo")
                    add_new_entry(output_dicts, src, arxiv_document)
                else:
                    print("nothing found - solo")
            except (MendeleyException, MendeleyApiException):
                print("MendeleyException DAFUCK WHAT WHAT")
                pass
            except Exception, e:
                print("HTTP?", str(e))


if __name__ == "__main__":
    mendeley_altmetrics()

