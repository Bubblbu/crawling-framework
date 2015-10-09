# Bibliometric crawling framework

A python framework to crawl various bibliometric sources.

[![DOI](https://zenodo.org/badge/18705/Bubblbu/crawling-framework.svg)](https://zenodo.org/badge/latestdoi/18705/Bubblbu/crawling-framework)

## Overview of all implemented interfaces

* **arXiv** - Inital data acquisition

  | Parameter | Description |
  | -------   | ------------- |
  | Input     | List of arxiv categories  |
  | Output    | DataFrame containing following variables: *arxiv_id, doi, title, authors, categories, primary_category, crawl_cat, journal_ref, submitted, updated*|

* **CrossRef** - Additional Identifiers

  | Parameter | Description |
  | -------   | ------------- |
  | Input     | DataFrame containing *title, authors, date* |
  | Output    | Input dataframe extended with *cr_doi, cr_title, lr* (levensthein_ratio) |

* **Mendeley** - Altmetrics

  | Parameter | Description |
  | -------   | ------------- |
  | Input     | DataFrame containing some identifiers (e.g.: *arxiv_id, arxiv_doi, cr_doi*)  |
  | Output    | DataFrame with bibliometric metadata, abstract, mendeley identifiers, mendeley readership data |

## Useful resources

+ ADS
  + ADS Blog and Help page: http://adsabs.github.io/help/
  + Official github for ADS Dev API: https://github.com/adsabs/adsabs-dev-api

## Troubleshooting

### R/rpy2 - Setup

In order to run the arxiv-crawler both R and rpy2 will need to be installed and setup correctly.
In case rpy2 fails to find the package "aRxiv", the following steps should help:

* Download rpy2-binaries [here](http://www.lfd.uci.edu/~gohlke/pythonlibs/#rpy2). Make sure to choose the correct version.

  In order to install the downloaded .whl file, do this: `pip install wheel` and then `pip install *.whl`
  
* Install the R-package "aRxiv" using install.packages("aRxiv") within a R-session
* Determine the locations of your R-libraries with .libPath() and ...

  ... add these locations to the variable **R_LIBS** (*"location1;location2;..."*)
  
* Add the variable **R_HOME** to the root of your R distro, e.g.: *"C:\Program Files\R\R-3.1.2"*
* Add the variable **R_USER** with your user-name as the value.

## Author

Asura Enkhbayar  <[aenkhbayar@know-center.at](aenkhbayar@know-center.at)>
