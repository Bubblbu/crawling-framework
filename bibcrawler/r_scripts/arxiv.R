library(aRxiv)

search_arxiv_submission_range <- function(subcat,
                         submittedDateStart, submittedDateEnd,
                         limit = 10, batchsize = 100,
                         start=0){
  submittedRange = paste("submittedDate:[",submittedDateStart," TO ",submittedDateEnd,"]",sep="")

  cat = paste("cat:",subcat,sep="")
  query = paste(cat,submittedRange,sep = " AND ")
  
  return(arxiv_search(query, limit=limit, batchsize=batchsize, start=start))
}

search_arxiv_update_range <- function(subcat,updateStart, updatedEnd,
                                      limit = 10, batchsize = 100,start=0){
  updatedRange = paste("submittedDate:[",updatedStart," TO ",updatedEnd,"]", sep="")
  cat = paste("cat:",subcat,sep="")
  
  query = paste(cat,updatedRange,sep = " AND ")
  
  return(arxiv_search(query, limit=limit, batchsize=batchsize, start=start))
}

search_arxiv <- function(subcat,limit = 10, batchsize = 100,start=0){
  cat = paste("cat:",subcat,sep="")
  query = cat
 
  return(arxiv_search(query, limit=limit, batchsize=batchsize, start=start))
}

search_arxiv_submission_update_range <- function(subcat,limit = 10, batchsize = 100,
                                                 submittedDateStart, submittedDateEnd,
                                                 updatedStart, updatedEnd,start=0){
  submittedRange = paste("submittedDate:[",submittedDateStart," TO ",submittedDateEnd,"]",sep="")
  updatedRange = paste("submittedDate:[",updatedStart," TO ",updatedEnd,"]", sep="")
  cat = paste("cat:",subcat,sep="")
  
  query = paste(cat,submittedRange,updatedRange,sep = " AND ")
  
  return(arxiv_search(query, limit=limit, batchsize=batchsize, start=start))
}

get_cat_count <- function(subcat)
{
  cat = paste("cat:",subcat,sep="")
  return(arxiv_count(cat))
}

set_delay <- function(delay){
  options(aRxiv_delay=delay)
}

set_toomany <- function(toomany){
  options(aRxiv_toomany=toomany)
}