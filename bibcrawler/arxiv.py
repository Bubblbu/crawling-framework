#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, division
from pprint import pprint
import cPickle as pickle
from collections import defaultdict
import csv
import re
import urllib2

import lxml.etree as ET
from urlgrabber.keepalive import HTTPHandler

__author__ = 'Asura Enkhbayar <asura.enkhbayar@gmail.com>'


# keepalive_handler = HTTPHandler()
# opener = urllib2.build_opener(keepalive_handler)
# urllib2.install_opener(opener)

""" arXiv categories + subcategories - read in from textfile """
# with open("resources/arxiv_categories.pickle", 'rb') as f:
# cats = pickle.load(f)

""" arXiv categories + subcategories - hardcoded dict """
cats = {'astro-ph': {'name': 'Astrophysics'},
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
        'quant-ph': {'name': 'Quantum Physics'},
        'stat': {'name': 'Statistics',
                 'subcats': {'AP': 'Applications',
                             'CO': 'Computation',
                             'ME': 'Methodology',
                             'ML': 'Machine Learning',
                             'TH': 'Theory'}}}


def arxiv_category_crawler(subcategory):
    item_list = []
    found_entries = 0
    start = 0
    results_per_page = 500

    doi_count = 0

    print("\n", subcategory)

    while True:
        print("------ {} out of {} ---------".format(start, start + results_per_page))
        url = "http://export.arxiv.org/api/query?search_query=cat:{}&start={}&max_results={}".format(subcategory, start,
                                                                                                     results_per_page)
        xml = urllib2.urlopen(url).read()
        et = ET.XML(xml)

        entries = et.xpath(anywhere, name='entry')

        if len(entries) is 0:
            break
        else:
            for entry in entries:
                print(entry.text)
                arxiv_id = entry.xpath(here, name='id')
                doi_result = entry.xpath(here, name='doi')
                title = entry.xpath(here, name='title')
                date = entry.xpath(here, name='published')
                journal_ref = entry.xpath(here, name='journal_ref')
                name_entries = entry.xpath(here, name='name')
                prim_cat = entry.xpath(here, name='arxiv:primary_category')
                check = lambda x: '' if not x else x[0].text.strip()

                arxiv_id = check(arxiv_id)
                doi_result = check(doi_result)
                title = check(title)
                journal_ref = check(journal_ref)

                date = check(date)
                date = date[:10].split("-")
                year = int(date[0])
                month = int(date[1])
                day = int(date[2])

                arxiv_id = re.findall("http://arxiv.org/abs/(.*)v[0-9]*", arxiv_id)[0]
                print(arxiv_id)

                authors = []
                for name in name_entries:
                    authors.append(name.text)

                item_list.append({'arxiv_id': arxiv_id,
                                  'doi': doi_result,
                                  'title': title,
                                  'year': year,
                                  'month': month,
                                  'day': day,
                                  'journal': journal_ref,
                                  'authors': authors})

                if doi_result:
                    doi_count += 1

            print("\t{} DOIs found in {}".format(doi_count, subcategory))
            found_entries += len(entries)
            # pprint(item_list, indent=5)

        start += results_per_page

    print("\nFound DOI's --- {} from {} results".format(doi_count, found_entries))

    with open("{}{}.pkl".format(PICKLE_DIR, subcategory), "wb") as f:
        pickle.dump(item_list, f)

    return item_list


    # def get_metadata_from_arxiv(category):
    # list_of_cat_dois = []
    # for subcat in categories[category]:
    #     try:
    #         with open("{}{}.pkl".format(PICKLE_DIR, subcat), "rb") as f:
    #             loaded = pickle.load(f)
    #         list_of_cat_dois.append(loaded)
    #         print "\nLoaded pre-fetched {}.pkl".format(subcat)
    #     except IOError:
    #         print "\n{}.pkl not found. Crawling...".format(subcat)
    #         list_of_cat_dois.append(arxiv_category_crawler(subcat))
    #     except EOFError:
    #         pass

    # return list_of_cat_dois