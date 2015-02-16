from __future__ import print_function, division
from pprint import pprint

__author__ = 'Asura Enkhbayar <asura.enkhbayar@gmail.com>'

from arxiv import r_arxiv_crawler
from doi_lookup import doi_lookup

data = r_arxiv_crawler("stat.AP", limit=10)

print("check")

dois = doi_lookup(data.authors, data.title, data.submitted)

pprint(dois)