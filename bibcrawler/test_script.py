from __future__ import print_function, division
from pprint import pprint

__author__ = 'Asura Enkhbayar <asura.enkhbayar@gmail.com>'

from arxiv import r_arxiv_crawler
from doi_lookup import doi_lookup

data = r_arxiv_crawler("stat.AP", limit=30)

pprint("------ STAGE 1 ------")
pprint(data.describe())

extended_data = doi_lookup(data)

pprint("------ STAGE 2 ------")
pprint(extended_data.describe())

pprint(extended_data[['crossref_doi','crossref_score']])