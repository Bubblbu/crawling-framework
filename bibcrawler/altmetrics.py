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


def init_temp(src):
    temp = dict()
    temp['arxiv_id'] = src['arxiv_id']
    temp['arxiv_doi'] = src['arxiv_doi']
    temp['crossref_doi'] = src['crossref_doi']
    temp['mndly_path'] = src['mndly_path']
    temp['submitted'] = src['submitted']

    temp['mndly_match'] = np.nan
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

    return temp


def add_new_entry(list_of_dicts, src, mndly_doc):
    temp = init_temp(src)

    # Check with arXiv data
    if not mndly_doc:
        pass
    else:
        if 'arxiv' in mndly_doc.identifiers:
            arxiv_id = src['arxiv_id']
            mndly_arxiv_id = mndly_doc.identifiers['arxiv']

            # TODO - remove the version number
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
                print("\t\tDocuments id's match")
                print("\t\t " + arxiv_id, " - ", mndly_arxiv_id)
                print("\t\t " + src['title'], " - ", mndly_doc.title)
                check = True
            else:
                print("\t\tId's Mismatch")
                print("\t\t " + arxiv_id, " - ", mndly_arxiv_id)
                print("\t\t " + src['title'], " - ", mndly_doc.title)
                check = False
        else:
            lr = levenshtein_ratio(src['title'], mndly_doc.title)
            if lr <= LR:
                print("\t\tDocuments titles match - {}".format(lr))
                print("\t\t " + src['title'], " - ", mndly_doc.title)
                check = True
            else:
                print("\t\tTitles Mismatch - {}".format(lr))
                print("\t\t " + src['title'], " - ", mndly_doc.title)
                check = False

        if check:
            temp['mndly_match'] = True
        else:
            temp['mndly_match'] = False

        temp['type'] = mndly_doc.type
        temp['identifier_count'] = len(mndly_doc.identifiers)
        temp['identifiers'] = "-".join(mndly_doc.identifiers)

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
        # else:
        #     if 'arxiv' in mndly_doc.identifiers:
        #         ret_val = (src['arxiv_id'], mndly_doc.identifiers['arxiv'])
        #     else:
        #         ret_val = (unicode(src['title']).encode('utf-8'), unicode(mndly_doc.title).encode('utf-8'))
        #
        #     temp['mndly_kicked'] = ret_val

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
    else:
        stage2_dir = stage1_dir + stage2_dir + "/"

    working_folder = stage2_dir + timestamp
    print(working_folder)
    os.makedirs(working_folder)

    # Read in stage 2 file
    input_df = pd.io.json.read_json(stage2_dir + "stage_2.json")
    input_df = input_df.fillna(value=np.nan)
    input_df = input_df.replace("", np.nan)

    output_dicts = []
    max_len = len(input_df.index)
    for count, (idx, row) in enumerate(input_df.iterrows()):
        arxiv_document = None
        doi_document = None

        print("\n=== FILE {} of {} ===".format(count, max_len))

        arxiv_id = row['id']
        found_regex = new_arxiv_format.findall(arxiv_id)
        if found_regex:
            arxiv_id = found_regex[0]
        else:
            found_regex = old_arxiv_format.findall(arxiv_id)
            if found_regex:
                arxiv_id = found_regex[0]
        print(arxiv_id)

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
               'submitted': row['submitted'],
               'mndly_path': np.nan}

        if doi:
            try:
                doi_document = session.catalog.by_identifier(doi=doi, view='all')
            except (MendeleyException, MendeleyApiException), e:
                pass
            except Exception, e:
                print("HTTP", str(e))

            try:
                arxiv_document = session.catalog.by_identifier(arxiv=arxiv_id, view='all')
            except (MendeleyException, MendeleyApiException), e:
                pass
            except Exception, e:
                print("HTTP", str(e))

            if doi_document and not arxiv_document:
                print("\t+ doi found, but no arxiv")
                src['mndly_path'] = 1
                add_new_entry(output_dicts, src, doi_document)

            elif not doi_document and arxiv_document:
                print("\t+ arxiv found, but no doi")
                src['mndly_path'] = 2
                add_new_entry(output_dicts, src, arxiv_document)

            elif not doi_document and not arxiv_document:
                print("\t+ None found")
                src['mndly_path'] = 7
                add_new_entry(output_dicts, src, None)
                continue

            else:
                print("\t+ both found")
                if doi_document.id == arxiv_document.id:
                    src['mndly_path'] = 3
                    add_new_entry(output_dicts, src, arxiv_document)
                else:
                    if len(doi_document.identifiers) >= len(arxiv_document.identifiers):
                        src['mndly_path'] = 4
                        add_new_entry(output_dicts, src, doi_document)
                    else:
                        src['mndly_path'] = 5
                        add_new_entry(output_dicts, src, arxiv_document)
        else:
            try:
                arxiv_document = session.catalog.by_identifier(arxiv=arxiv_id, view='all')
                if arxiv_document:
                    print("\t+ arxiv found - solo")
                    src['mndly_path'] = 6
                    add_new_entry(output_dicts, src, arxiv_document)
            except (MendeleyException, MendeleyApiException), e:
                print("\t+ nothing found - solo")
                src['mndly_path'] = 8
                add_new_entry(output_dicts, src, None)
                pass
            except Exception, e:
                print("HTTP?", str(e))

    output = pd.DataFrame(output_dicts)
    output.to_json(working_folder + "/stage_3.json")


if __name__ == "__main__":
    mendeley_altmetrics()

