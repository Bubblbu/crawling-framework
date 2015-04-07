from __future__ import print_function, division

from arxiv import r_arxiv_crawler
from utils import get_arxiv_subcats
from temp_doi import doi_lookup
# from doi_lookup import doi_lookup
from altmetrics import mendeley_altmetrics

__author__ = 'Asura Enkhbayar <asura.enkhbayar@gmail.com>'

crawling_list = get_arxiv_subcats(['nlin'])
# crawling_list = {"cd": ["cs.AR", "stat.AP", "cs.AI"]}

print(crawling_list)

#  === STAGE 1 ===
r_arxiv_crawler(crawling_list, batchsize=400, delay=1)

#  === STAGE 2 ===
# doi_lookup(9, stage1_dir="2015-03-18_18-10-37", num_workers=10)

#  === STAGE 3 ===
i = 9
# mendeley_altmetrics(stage1_dir="astro-ph/again_bitch", stage2_dir="temp_{}".format(i))