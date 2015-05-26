"""
.. module:: Test script for crawling framework
   :platform: Windows
   :synopsis: Fiddle with this.

.. moduleauthor:: Asura Enkhbayar <aenkhbayar@gmail.com>
"""

from __future__ import print_function, division
from arxiv import *
from doi_lookup import *
from altmetrics import *

__author__ = 'Asura Enkhbayar <asura.enkhbayar@gmail.com>'

if __name__ == '__main__':
    # Category to crawl - nlin is quite big (not as big as others... but still big...)
    crawling_list = get_arxiv_subcats(['nlin'])

    # If you want to test with a smaller
    # crawling_list = {"cs": ["cs.GR"]}

    # === STAGE 1 ===
    folder = r_arxiv_crawler(crawling_list, batchsize=400, delay=1)
    # Only use if the merging of the temporary files leads to OutOfMemory...
    # TODO Deal with OutOfMemory during concatenation
    # folder = test_merge(1, "2015-04-20_18-28-05")

    arxiv_cleanup(folder)

    # === STAGE 2 ===
    # Maybe even more proccesses are ok... Havent benchmarked all the parallel stuff yet
    folder = doi_lookup(input_folder=None, num_processes=4, num_threads=10)
    doi_cleanup(folder)

    # === STAGE 3 ===
    mendeley_altmetrics(stage1_dir=None, stage2_dir=None, num_threads=10)