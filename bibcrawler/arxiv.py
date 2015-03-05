#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, division

import os
import time
import datetime
import numpy as np
import pandas as pd

from rpy2.robjects.packages import SignatureTranslatedAnonymousPackage
import pandas.rpy.common as com

from config import base_directory


__author__ = 'Asura Enkhbayar <asura.enkhbayar@gmail.com>'

# """ arXiv categories + subcategories - read in from textfile """
# with open("resources/arxiv_categories.pickle", 'rb') as f:
# cats = pickle.load(f)

#: All arxiv categories and their subcategories
ARXIV_CATS = {'astro-ph': {'name': 'Astrophysics',
                           'subcats': {'GA': 'Astrophysics of Galaxies',
                                       'CO': 'Cosmology and Nongalactic Astrophysics',
                                       'EP': 'Earth and Planetary Astrophysics',
                                       'HE': 'High Energy Astrophysical Phenomena',
                                       'IM': 'Instrumentation and Methods for Astrophysics',
                                       'SR': 'Solar and Stellar Astrophysics'}},
              'cond-mat': {'name': 'Physics',
                           'subcats': {'dis-nn': 'Disordered Systems and Neural Networks',
                                       'mes-hall': 'Mesoscopic Systems and Quantum Hall Effect',
                                       'mtrl-sci': 'Materials Science',
                                       'other': 'Other',
                                       'soft': 'Soft Condensed Matter',
                                       'stat-mech': 'Statistical Mechanics',
                                       'str-el': 'Strongly Correlated Electrons',
                                       'supr-con': 'Superconductivity'}},
              'cs': {'name': 'Computer Science',
                     'subcats': {'AI': 'Artificial Intelligence',
                                 'AR': 'Architecture',
                                 'CC': 'Computational Complexity',
                                 'CE': 'Computational Engineering; Finance; and Science',
                                 'CG': 'Computational Geometry',
                                 'CL': 'Computation and Language',
                                 'CR': 'Cryptography and Security',
                                 'CV': 'Computer Vision and Pattern Recognition',
                                 'CY': 'Computers and Society',
                                 'DB': 'Databases',
                                 'DC': 'Distributed; Parallel; and Cluster Computing',
                                 'DL': 'Digital Libraries',
                                 'DM': 'Discrete Mathematics',
                                 'DS': 'Data Structures and Algorithms',
                                 'GL': 'General Literature',
                                 'GR': 'Graphics',
                                 'GT': 'Computer Science and Game Theory',
                                 'HC': 'Human-Computer Interaction',
                                 'IR': 'Information Retrieval',
                                 'IT': 'Information Theory',
                                 'LG': 'Learning',
                                 'LO': 'Logic in Computer Science',
                                 'MA': 'Multiagent Systems',
                                 'MM': 'Multimedia',
                                 'MS': 'Mathematical Software',
                                 'NA': 'Numerical Analysis',
                                 'NE': 'Neural and Evolutionary Computing',
                                 'NI': 'Networking and Internet Architecture',
                                 'OH': 'Other',
                                 'OS': 'Operating Systems',
                                 'PF': 'Performance',
                                 'PL': 'Programming Languages',
                                 'RO': 'Robotics',
                                 'SC': 'Symbolic Computation',
                                 'SD': 'Sound',
                                 'SE': 'Software Engineering'}},
              'gr-qc': {'name': 'General Relativity and Quantum Cosmology'},
              'hep-ex': {'name': 'High Energy Physics'},
              'hep-lat': {'name': 'High Energy Physics'},
              'hep-ph': {'name': 'High Energy Physics'},
              'hep-th': {'name': 'High Energy Physics'},
              'math': {'name': 'Mathematics',
                       'subcats': {'AC': 'Commutative Algebra',
                                   'AG': 'Algebraic Geometry',
                                   'AP': 'Analysis of PDEs',
                                   'AT': 'Algebraic Topology',
                                   'CA': 'Classical Analysis and ODEs',
                                   'CO': 'Combinatorics',
                                   'CT': 'Category Theory',
                                   'CV': 'Complex Variables',
                                   'DG': 'Differential Geometry',
                                   'DS': 'Dynamical Systems',
                                   'FA': 'Functional Analysis',
                                   'GM': 'General Mathematics',
                                   'GN': 'General Topology',
                                   'GR': 'Group Theory',
                                   'GT': 'Geometric Topology',
                                   'HO': 'History and Overview',
                                   'IT': 'Information Theory',
                                   'KT': 'K-Theory and Homology',
                                   'LO': 'Logic',
                                   'MG': 'Metric Geometry',
                                   'MP': 'Mathematical Physics',
                                   'NA': 'Numerical Analysis',
                                   'NT': 'Number Theory',
                                   'OA': 'Operator Algebras',
                                   'OC': 'Optimization and Control',
                                   'PR': 'Probability',
                                   'QA': 'Quantum Algebra',
                                   'RA': 'Rings and Algebras',
                                   'RT': 'Representation Theory',
                                   'SG': 'Symplectic Geometry',
                                   'SP': 'Spectral Theory',
                                   'ST': 'Statistics'}},
              'math-ph': {'name': 'Mathematical Physics'},
              'nlin': {'name': 'Nonlinear Sciences',
                       'subcats': {'AO': 'Adaptation and Self-Organizing Systems',
                                   'CD': 'Chaotic Dynamics',
                                   'CG': 'Cellular Automata and Lattice Gases',
                                   'PS': 'Pattern Formation and Solitons',
                                   'SI': 'Exactly Solvable and Integrable Systems'}},
              'nucl-ex': {'name': 'Nuclear Experiment'},
              'nucl-th': {'name': 'Nuclear Theory'},
              'physics': {'name': 'Physics',
                          'subcats': {'acc-ph': 'Accelerator Physics',
                                      'ao-ph': 'Atmospheric and Oceanic Physics',
                                      'atm-clus': 'Atomic and Molecular Clusters',
                                      'atom-ph': 'Atomic Physics',
                                      'bio-ph': 'Biological Physics',
                                      'chem-ph': 'Chemical Physics',
                                      'class-ph': 'Classical Physics',
                                      'comp-ph': 'Computational Physics',
                                      'data-an': 'Data Analysis; Statistics and Probability',
                                      'ed-ph': 'Physics Education',
                                      'flu-dyn': 'Fluid Dynamics',
                                      'gen-ph': 'General Physics',
                                      'geo-ph': 'Geophysics',
                                      'hist-ph': 'History of Physics',
                                      'ins-det': 'Instrumentation and Detectors',
                                      'med-ph': 'Medical Physics',
                                      'optics': 'Optics',
                                      'plasm-ph': 'Plasma Physics',
                                      'pop-ph': 'Popular Physics',
                                      'soc-ph': 'Physics and Society',
                                      'space-ph': 'Space Physics'}},
              'q-bio': {'name': 'Quantitative Biology',
                        'subcats': {'BM': 'Biomolecules',
                                    'CB': 'Cell Behavior',
                                    'GN': 'Genomics',
                                    'MN': 'Molecular Networks',
                                    'NC': 'Neurons and Cognition',
                                    'OT': 'Other',
                                    'PE': 'Populations and Evolution',
                                    'QM': 'Quantitative Methods',
                                    'SC': 'Subcellular Processes',
                                    'TO': 'Tissues and Organs'}},
              'q-fin': {'name': 'Quantitative Finance',
                        'subcats': {'CP': 'Computational Finance',
                                    'EC': 'Economics',
                                    'GN': 'General Finance',
                                    'MF': 'Mathematical Finance',
                                    'PM': 'Portfolio Management',
                                    'PR': 'Pricing of Securities',
                                    'RM': 'Risk Management',
                                    'ST': 'Statistical Finance',
                                    'TR': 'Trading and Market Microstructure'}},
              'quant-ph': {'name': 'Quantum Physics'},
              'stat': {'name': 'Statistics',
                       'subcats': {'AP': 'Applications',
                                   'CO': 'Computation',
                                   'ME': 'Methodology',
                                   'ML': 'Machine Learning',
                                   'TH': 'Theory'}}}


def get_arxiv_subcats(cats):
    subcategories = {}
    for cat in cats:
        subcategories[cat] = [cat + "." + subcat for subcat in ARXIV_CATS[cat]['subcats'].keys()]

    return subcategories


def get_subcat_fullname(subcat):
    if "." in subcat:
        parts = subcat.split(".")
        name = ARXIV_CATS[parts[0]]['name']
        subname = ARXIV_CATS[parts[0]]['subcats'][parts[1]]

        return unicode(name + " - " + subname)


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

    print("Created new folder: <<" + working_folder + ">>")

    # Load R-scripts
    print("Loading R-Scripts ...")
    with open('r_scripts/arxiv.R', 'r') as f:
        string = ''.join(f.readlines())
    arxiv_crawler = SignatureTranslatedAnonymousPackage(string, "arxiv_crawler")

    # arxiv_delay
    if delay:
        arxiv_crawler.set_delay(delay)

    # Crawling
    crawl_log = pd.DataFrame(columns=["Cat.Abb", "Entries on arxiv.org", "Entries found", "Time", "Full Name"])

    temp_count = 0
    for cat, subcats in crawling_list.iteritems():
        print("Crawling " + cat + ":")
        for subcategory in subcats:
            print("\t" + subcategory)
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
                print("\t\tBatch {} out of {}".format(count, max_count))
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
                                print("\t\t\t NOT ENOUGH DATA RECEIVED... Retry {}".format(try_count))

                                # TODO EXCEPTION HANDLING
                                continue

                        subcat_df = pd.concat([subcat_df, batch])
                        break

                        # break

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
            temp_count += 1

    ts_finish = time.time()

    # Create log files

    crawl_log.to_csv(working_folder + "/crawl_log.csv", sep=";")
    write_log(working_folder, ts_start, ts_finish)

    # # Combine all the temporary files - NOT WORKING. Memory errors
    # result_df = pd.DataFrame()
    # temp_dfs = []
    # try:
    #     for i in range(0, temp_count):
    #         print(working_folder + "/temp_{}.json".format(i))
    #         # temp_dfs.append(pd.io.json.read_json(working_folder + "/temp_{}.json".format(i)))
    #         result_df = pd.concat([result_df, pd.io.json.read_json(working_folder + "/temp_{}.json".format(i))])
    #     result_df = pd.concat(temp_dfs)
    #
    #     result_df.index = range(0, len(result_df.index))
    #     result_df.to_json(working_folder + "/stage_1.json")
    # except Exception, e:
    #     print(str(e))

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