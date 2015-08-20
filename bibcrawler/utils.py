#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, division
import re
import pandas as pd
from numpy import nan
from Levenshtein import distance

#: Levenshtein Ratio - Schloegl et al, 2014
LR = 1 / 15.83

#: Regex for 'only alpha-numeric'
regex_alphanum = re.compile(r"[^a-zA-Z0-9]")
#: Regex for more than 2 whitespaces
regex_mult_whitespace = re.compile(r"\s{2,}")

#: DOI Regex
regex_doi = re.compile(r'\b(10[.][0-9]{4,}(?:[.][0-9]+)*/(?:(?!["&\'<>])\S)+)\b')
#: Old Arxiv Regex
regex_old_arxiv = re.compile(r'(?:ar[X|x]iv:)?([^\/]+\/\d+)(?:v\d+)?$')
#: New Arxiv Regex
regex_new_arxiv = re.compile(r'(?:ar[X|x]iv:)?(\d{4}\.\d{4,5})(?:v\d+)?$')


def levenshtein_ratio(string1, string2):
    """
    Calculates levenshtein ratio between two strings
    :param string1: First string
    :param string2: Second string
    :return: Levenshtein Ratio
    """
    if type(string1) is not unicode:
        string1 = ""
    if type(string2) is not unicode:
        string2 = ""

    original_edited = regex_alphanum.sub(" ", string1).strip()
    original_edited = regex_mult_whitespace.sub(" ", original_edited).lower()

    found_edited = regex_alphanum.sub(" ", string2).strip()
    found_edited = regex_mult_whitespace.sub(" ", found_edited).lower()

    ld = distance(unicode(original_edited), unicode(found_edited))
    max_len = max(len(original_edited), len(found_edited))

    if max_len == 0:
        return 1
    return ld / max_len

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
              'hep-ex': {'name': 'High Energy Physics - Experiment'},
              'hep-lat': {'name': 'High Energy Physics - Lattice'},
              'hep-ph': {'name': 'High Energy Physics - Phenomenology'},
              'hep-th': {'name': 'High Energy Physics - Theory'},
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
    """
    List of cats -> List of subcats "cat.subcat"
    :param cats: List of categories
    :return: List of categorical abbrevations of all subcategories
    """
    subcategories = {}
    for cat in cats:
        subcategories[cat] = [cat + "." + subcat for subcat in ARXIV_CATS[cat]['subcats'].keys()]

    return subcategories


def get_subcat_fullname(subcat):
    """
    Returns a human readable description of the subcategory
    :param subcat: Subcategory
    :return: Full name category - Full name subcategory
    """
    if "." in subcat:
        parts = subcat.split(".")
        name = ARXIV_CATS[parts[0]]['name']
        subname = ARXIV_CATS[parts[0]]['subcats'][parts[1]]
        return unicode(name + " - " + subname)
    else:
        return ARXIV_CATS[subcat]['name']




def clean_dataset(df, logger, earliest_date, latest_date,
                  remove_columns):
    """
    General function to clean panda dataframes

    :param df: Input dataframe
    :param logger: Logger
    :param earliest_date: Earliest possible date
    :param latest_date: Latest possible date
    :param remove_columns: List of col-names to be removed
    :return: Cleaned dataframe
    """
    # Remove columns
    if remove_columns:
        logger.info("Removing columns")
        for col in remove_columns:
            if col in df:
                del df[col]
            else:
                logger.error("Column \"{}\" not in dataframe".format(col))

    # Strip all columns
    logger.info("Stripping all entries")

    def clean(a):
        try:
            a = a.str.replace(r"\n", " ")
            a = a.str.replace(r"\r", " ")
        except AttributeError:
            pass
        return a

    for col in df.columns:
        df[col] = clean(df[col])

    # Replace None & empty string
    logger.info("Replacing None & empty string with np.nan")
    df.fillna(nan, inplace=True)
    df = df.replace("", nan)

    # Remove duplicate entries based on arxiv id's
    logger.info("Removing duplicate entries")
    dupls = df.duplicated(subset=['id'])
    duplicate_row_indices = df[dupls].index

    df_new = df.drop(duplicate_row_indices)

    # Change date types to datetime
    df_new['submitted'] = pd.to_datetime(df_new['submitted'], unit="ms")
    df_new['updated'] = pd.to_datetime(df_new['updated'], unit="ms")

    # Apply date range based on submisssion date if applicable
    logger.info("Applying date ranges")
    if earliest_date:
        df_new = df_new[[date > earliest_date for date in df_new.submitted]]
    if latest_date:
        df_new = df_new[[date < latest_date for date in df_new.submitted]]

    # Save output
    df_new.index = range(0, len(df_new.index))

    return df_new