from __future__ import print_function, division

from arxiv import r_arxiv_crawler, get_arxiv_subcats
from doi_lookup import doi_lookup
from altmetrics import mendeley_altmetrics

__author__ = 'Asura Enkhbayar <asura.enkhbayar@gmail.com>'

crawling_list = get_arxiv_subcats(['astro-ph'])
# crawling_list = {"cd": ["cs.AR", "stat.AP", "cs.AI"]}

#  === STAGE 1 ===
# r_arxiv_crawler(crawling_list, batchsize=400, delay=1)

#  === STAGE 2 ===
doi_lookup(stage1_dir="cs_stat_6mb", num_workers=20)

#  === STAGE 3 ===
mendeley_altmetrics(stage1_dir="cs_stat_6mb")