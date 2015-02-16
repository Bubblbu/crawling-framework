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

def doi_lookup(list_of_authors, list_of_titles, list_of_submission_dates, mode='all'):
    """
    DOI Lookup interfaces to different DOI providers.
    Currently implemented: CrossRef.
    To-Do: DataCite

    Possible candidate documents are matched with original arxiv-documents using Levenshtein Ratio (Schloegl et al)

    :param list_of_authors: List of authors.
    :param list_of_titles: List of titles.
    :param list_of_submission_dates: List of submission dates.

    :returns: pd.DataFrame - newly found DOI's with original indices
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

    # TODO: Implement Levenshtein Ratio before returning results.

    for original, found in zip(list_of_titles, result.title):
        original = re.sub(r"[^a-zA-Z0-9]", " ", original).strip()
        original = re.sub(r"\s{2,}", " ", original).lower()
        found = re.sub(r"[^a-zA-Z0-9]", " ", found).strip()
        found = re.sub(r"\s{2,}", " ", found).lower()

        ld = distance(unicode(original), unicode(found))
        max_len = max(len(original),len(found))

        if ld/max_len <= 1/15.83:
            print("---- MATCH ---- ", ld/max_len)
            print(original)
            print(found)
            print("\n\n")
        else:
            print("---- FAIL ---- ", ld/max_len)
            print(original)
            print(found)
            print("\n\n")

    return result