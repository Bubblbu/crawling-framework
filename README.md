# Bibliometric crawling framework

A python framework to crawl various bibliometric sources.

## Overview

1. **Source** - Inital data acquisition

 The initial dataset is acquired at this point. Currently arxiv-subcategories can be crawled.
 
 Currently implemented: *arxiv.com*
	
2. **Enrichment** - Additional Identifiers

  Using APIs offered by DOI Registration Agencies metadata from the source (authors, titles, year) additional metadata is crawled. (DOIs)
  
  Currently implemented: *crossref.org*
	
3. **Output** - Altmetric/Science2.0 stuff

  Mendeley, Altmetric.com, ADS Harvard ...

  Currently implemented: *mendeley.com*



## R/rpy2 - Setup

In order to run the arxiv-crawler both R and rpy2 will need to be installed and setup correctly.
In case rpy2 fails to find the package "aRxiv", the following steps should help:

* Install the R-package "aRxiv" using install.packages("aRxiv") within a R-session
* Determine the locations of your R-libraries with .libPath() and ...

  ... add these locations to the variable **R_LIBS** (*"location1;location2;..."*)
  
* Add the variable **R_HOME** to the root of your R distro, e.g.: *"C:\Program Files\R\R-3.1.2"*
* Add the variable **R_USER** with your user-name as the value.

## Author

Asura Enkhbayar  <[aenkhbayar@know-center.at](aenkhbayar@know-center.at)>
