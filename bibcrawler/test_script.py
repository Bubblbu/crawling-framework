from __future__ import print_function, division
from arxiv import *
from utils import get_arxiv_subcats
from doi_lookup import *
# from doi_lookup import doi_lookup
from altmetrics import mendeley_altmetrics

__author__ = 'Asura Enkhbayar <asura.enkhbayar@gmail.com>'

if __name__ == '__main__':
    crawling_list = get_arxiv_subcats(['astro-ph'])
    # crawling_list = {"cs": ["cs.GR"]}

    # === STAGE 1 ===
    # folder = r_arxiv_crawler(crawling_list, batchsize=400, delay=1)
    folder = test_merge(6, "2015-04-14_19-53-57")

    arxiv_cleanup(folder)


    # === STAGE 2 ===
    folder = doi_lookup(input_folder=None, num_processes=4, num_threads=10)
    doi_cleanup(folder)
    # === STAGE 3 ===
    i = 9
    # mendeley_altmetrics(stage1_dir="astro-ph/again_bitch", stage2_dir="temp_{}".format(i))