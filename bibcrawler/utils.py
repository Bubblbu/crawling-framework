from __future__ import print_function, division
import re
from Levenshtein import distance

#: Levenshtein Ratio - Schloegl et al, 2014
LR = 1 / 15.83

# Regex for 'only alpha-numeric'
regex_alphanum = r"[^a-zA-Z0-9]"
regex_mult_whitespace = r"\s{2,}"

doi_check = re.compile(r'\b(10[.][0-9]{4,}(?:[.][0-9]+)*/(?:(?!["&\'<>])\S)+)\b')
old_arxiv_format = re.compile(r'(?:ar[X|x]iv:)?([^\/]+\/\d+)(?:v\d+)?$')
new_arxiv_format = re.compile(r'(?:ar[X|x]iv:)?(\d{4}\.\d{4})(?:v\d+)?$')


def levenshtein_ratio(string1, string2):
    original_edited = re.sub(regex_alphanum, " ", string1).strip()
    original_edited = re.sub(regex_mult_whitespace, " ", original_edited).lower()

    found_edited = re.sub(regex_alphanum, " ", string2).strip()
    found_edited = re.sub(regex_mult_whitespace, " ", found_edited).lower()

    ld = distance(unicode(original_edited), unicode(found_edited))
    max_len = max(len(original_edited), len(found_edited))

    return ld/max_len
