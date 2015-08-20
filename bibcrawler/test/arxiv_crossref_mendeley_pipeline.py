#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
.. module:: Test script for crawling framework
   :platform: Windows
   :synopsis: Fiddle with this.

.. moduleauthor:: Asura Enkhbayar <aenkhbayar@gmail.com>
"""

from __future__ import print_function, division
from api_interfaces.arxiv import arxiv_crawl, arxiv_cleanup, test_merge
from api_interfaces.crossref import crossref_crawl, crossref_cleanup
from api_interfaces.mendeley_api import mendeley_crawl, mendeley_cleanup

from utils import get_arxiv_subcats

if __name__ == '__main__':
    # Category to crawl - nlin is quite big (not as big as others... but still big...)
    # crawling_list = get_arxiv_subcats(['cs'])

    # If you want to test with a smaller
    # crawling_list = {"q-fin": ["q-fin.PR"]}


    # === STAGE 1 ===
    # arxiv_folder = arxiv_crawl(crawling_list, batchsize=400, delay=None)
    start_folder = r"E:\Work\Know-Center\CrawlingFramework\files\2015-07-27_12-39-28_pyhsics"

    # arxiv_cleanup(arxiv_folder)

    # Only use if the merging of the temporary files leads to OutOfMemory...
    # TODO Deal with OutOfMemory during concatenation
    # arxiv_folder = test_merge(start_folder)
    # arxiv_cleanup(arxiv_folder)

    # === STAGE 2 ===
    # Maybe even more proccesses are ok... Havent benchmarked all the parallel stuff yet

    continue_folder = r"E:\Work\Know-Center\CrawlingFramework\files\2015-07-27_12-39-28_pyhsics\2015-08-07_14-29-36"
    cr_folder = crossref_crawl(input_folder=start_folder, num_processes=6, num_threads=5,
                   continue_folder=continue_folder)
    crossref_cleanup(cr_folder)

    # === STAGE 3 ===
    mndly_folder = mendeley_crawl(stage1_dir=start_folder, stage2_dir=None, num_threads=15)
    mendeley_cleanup(mndly_folder)