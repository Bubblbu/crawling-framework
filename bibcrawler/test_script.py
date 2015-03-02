from __future__ import print_function, division
from pprint import pprint

from arxiv import r_arxiv_crawler, get_arxiv_subcats
from doi_lookup import doi_lookup

__author__ = 'Asura Enkhbayar <asura.enkhbayar@gmail.com>'

# crawling_list = get_arxiv_subcats(['astro-ph'])
crawling_list = {"stat": ["stat.AP"]}

r_arxiv_crawler(crawling_list, batchsize=400, delay=1)
# doi_lookup()

# pprint("------ STAGE 2 ------")
# pprint(extended_data.describe())
#
# pprint(extended_data[['crossref_doi', 'levenshtein_ratio']])