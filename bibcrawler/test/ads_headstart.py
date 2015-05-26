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
from api_interfaces.ads_api import ads_crawl

from processing.headstart import create_headstart_files

from utils import get_arxiv_subcats


if __name__ == "__main__":
    # crawling_list = get_arxiv_subcats(['astro-ph'])
    #
    # # If you want to test with a smaller
    # # crawling_list = {"q-fin": ["q-fin.PR"]}
    #
    # # === STAGE 1 ===
    # folder = arxiv_crawl(crawling_list, batchsize=400, delay=None)
    # arxiv_cleanup(folder)
    folder = "E:/Work/Know-Center/CrawlingFramework/files/cs_dl/2015-04-28_18-26-06/2015-04-28_18-30-33"
    new_folder = ads_crawl(folder, 100, 10)

    create_headstart_files(new_folder)


