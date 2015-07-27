#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
.. module:: Test script for data-ads-headstart pipeline
   :platform: Windows
   :synopsis: Fiddle with this.

.. moduleauthor:: Asura Enkhbayar <aenkhbayar@gmail.com>
"""

from __future__ import print_function, division
from api_interfaces.arxiv import arxiv_crawl, arxiv_cleanup
from api_interfaces.ads_api import ads_crawl_dataset, ads_crawl_category

from processing.headstart import create_headstart_files

from utils import get_arxiv_subcats, get_subcat_fullname


if __name__ == "__main__":
    # crawling_list = get_arxiv_subcats(['astro-ph'])
    #
    # # If you want to test with a smaller
    # # crawling_list = {"q-fin": ["q-fin.PR"]}
    #
    # # === STAGE 1 ===
    # folder = arxiv_crawl(crawling_list, batchsize=400, delay=None)
    # arxiv_cleanup(folder)
    cats = get_arxiv_subcats(['q-fin', 'stat', 'nlin', 'physics'])
    full_names = []
    for cat in cats.values():
        full_names.append([get_subcat_fullname(subcat) for subcat in cat])
    # full_names.extend([[get_subcat_fullname(subcat) for subcat in ['hep-ex', 'hep-lat', 'hep-ph', 'hep-th']]])

    for cat_names in full_names:
        print("CRAWLING {} CATS".format(len(cat_names)))
        print(cat_names)
        new_folder = ads_crawl_category(cat_names, 500, 10)

        # create_headstart_files(new_folder)


