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
from api_interfaces.mendeley_api import mendeley_crawl

from utils import get_arxiv_subcats

if __name__ == '__main__':
    # Category to crawl - nlin is quite big (not as big as others... but still big...)
    # crawling_list = get_arxiv_subcats(['cs'])

    # If you want to test with a smaller
    # crawling_list = {"q-fin": ["q-fin.PR"]}

    # folder = r"E:\Work\Know-Center\CrawlingFramework\files\2015-07-23_13-10-23"

    # === STAGE 1 ===
    # folder = arxiv_crawl(crawling_list, batchsize=400, delay=None)
    # arxiv_cleanup(folder)

    temp_folder = "2015-07-23_13-10-23_math"

    # Only use if the merging of the temporary files leads to OutOfMemory...
    # TODO Deal with OutOfMemory during concatenation
    # folder = test_merge(temp_folder)
    # arxiv_cleanup(folder)

    # === STAGE 2 ===
    # Maybe even more proccesses are ok... Havent benchmarked all the parallel stuff yet
    continue_folder =  None
    folder = crossref_crawl(input_folder=temp_folder, num_processes=4, num_threads=10, continue_folder=continue_folder)
    # crossref_cleanup(folder)

    # === STAGE 3 ===
    # mendeley_crawl(stage1_dir=temp_folder, stage2_dir=None, num_threads=10)
