#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
.. module:: Test script for crawling framework
   :platform: Windows
   :synopsis: Fiddle with this.

.. moduleauthor:: Asura Enkhbayar <aenkhbayar@gmail.com>
"""

from __future__ import print_function, division
from api_interfaces.arxiv import arxiv_crawl, arxiv_cleanup
from api_interfaces.crossref import crossref_crawl, crossref_cleanup
from api_interfaces.mendeley_api import mendeley_crawl

from utils import get_arxiv_subcats

if __name__ == '__main__':
    # Category to crawl - nlin is quite big (not as big as others... but still big...)
    crawling_list = get_arxiv_subcats(['nlin'])

    # If you want to test with a smaller
    # crawling_list = {"cs": ["cs.GR"]}

    # === STAGE 1 ===
    folder = arxiv_crawl(crawling_list, batchsize=400, delay=1)
    # Only use if the merging of the temporary files leads to OutOfMemory...
    # TODO Deal with OutOfMemory during concatenation
    # folder = test_merge(1, "2015-04-20_18-28-05")

    arxiv_cleanup(folder)

    # === STAGE 2 ===
    # Maybe even more proccesses are ok... Havent benchmarked all the parallel stuff yet
    folder = crossref_crawl(input_folder=None, num_processes=4, num_threads=10)
    crossref_cleanup(folder)

    # === STAGE 3 ===
    mendeley_crawl(stage1_dir=None, stage2_dir=None, num_threads=10)