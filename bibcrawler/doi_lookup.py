#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, division
from pprint import pprint

from Levenshtein import distance

from rpy2.robjects.packages import SignatureTranslatedAnonymousPackage

import pandas as pd
import pandas.rpy.common as com
import re

__author__ = 'Asura Enkhbayar <asura.enkhbayar@gmail.com>'

#: Levenshtein Ratio - Schloegl et al, 2014
LR = 1 / 15.83

# Regex for 'only alpha-numeric'
regex_alphanum = r"[^a-zA-Z0-9]"
regex_mult_whitespace = r"\s{2,}"


def crossref_lookup(authors, titles, submitted):
    cr_lookup = None

    with open('r_scripts/doi_lookup.R', 'r') as f:
        string = ''.join(f.readlines())
    doi_lookuper = SignatureTranslatedAnonymousPackage(string, "doi_lookuper")

    for author, title, date in zip(authors, titles, submitted):
        if cr_lookup is None:
            cr_lookup = com.convert_robj(doi_lookuper.crossref(author, title, date))
        else:
            cr_lookup = pd.concat([cr_lookup, com.convert_robj(doi_lookuper.crossref(author, title, date))])

    cr_dois = []
    levenshtein_ratio = []
    for original, found, doi in zip(titles, cr_lookup.title, cr_lookup.DOI):
        original = re.sub(regex_alphanum, " ", original).strip()
        original = re.sub(regex_mult_whitespace, " ", original).lower()

        found = re.sub(regex_alphanum, " ", found).strip()
        found = re.sub(regex_mult_whitespace, " ", found).lower()

        ld = distance(unicode(original), unicode(found))
        max_len = max(len(original), len(found))

        if ld / max_len <= LR:
            cr_dois.append(doi)
        else:
            cr_dois.append(None)

        levenshtein_ratio.append(ld / max_len)

    return cr_dois, levenshtein_ratio


def doi_lookup(arxiv_df, mode='all'):
    """
    DOI Lookup interfaces to different DOI providers.
    Currently implemented: CrossRef.
    To-Do: DataCite

    Possible candidate documents are matched with original arxiv-documents using Levenshtein Ratio (Schloegl et al)

    :param arxiv_df: The arxiv dataframe that needs to be enriched with more DOIs
    :type arxiv_df: pd.DataFrame

    :returns: pd.DataFrame - newly found DOI's with original indices
    """

    extended_df = arxiv_df.copy(deep=True)
    cr_dois = []
    levenshtein_ratio = []

    if mode == 'all':
        cr_dois, levenshtein_ratio = crossref_lookup(arxiv_df.authors, arxiv_df.title, arxiv_df.submitted)

    elif mode == 'crossref':
        cr_dois = crossref_lookup(arxiv_df.authors, arxiv_df.titles, arxiv_df.submitted)

    elif mode == 'datacite':
        pass

    extended_df['crossref_doi'] = pd.Series(cr_dois)
    extended_df['levenshtein_ratio'] = pd.Series(levenshtein_ratio)

    return extended_df