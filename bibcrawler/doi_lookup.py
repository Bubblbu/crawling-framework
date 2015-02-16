#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, division
from pprint import pprint

from rpy2.robjects.packages import SignatureTranslatedAnonymousPackage

import pandas as pd
import pandas.rpy.common as com

__author__ = 'Asura Enkhbayar <asura.enkhbayar@gmail.com>'


def doi_lookup(list_of_authors, list_of_titles, list_of_submission_dates, mode='all'):
    """
    This is a python wrapper for the aRxiv "arxiv_search" function.

    """

    with open('r_scripts/doi_lookup.R', 'r') as f:
        string = ''.join(f.readlines())
    doi_lookuper = SignatureTranslatedAnonymousPackage(string, "doi_lookuper")

    result = None
    if mode == 'all':
        for author, title, date in zip(list_of_authors, list_of_titles, list_of_submission_dates):
            if result is None:
                result = com.convert_robj(doi_lookuper.crossref(author, title, date))
            else:
                result = pd.concat([result, com.convert_robj(doi_lookuper.crossref(author, title, date))])

    return result