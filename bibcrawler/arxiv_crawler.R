library(aRxiv)
search_arxiv_submission_range <- function(subcat,
                         submittedDateStart, submittedDateEnd,
                         limit = 10, batchsize = 100){
  submittedRange = paste("submittedDate:[",submittedDateStart," TO ",submittedDateEnd,"]",sep="")

  cat = paste("cat:",subcat,sep="")
  query = paste(cat,submittedRange,sep = " AND ")
  
  return(arxiv_search(query, limit=limit, batchsize =batchsize))
}

search_arxiv_update_range <- function(subcat,updateStart, updatedEnd,
                                      limit = 10, batchsize = 100){
  updatedRange = paste("submittedDate:[",updatedStart," TO ",updatedEnd,"]", sep="")
  cat = paste("cat:",subcat,sep="")
  
  query = paste(cat,updatedRange,sep = " AND ")
  
  return(arxiv_search(query, limit=limit, batchsize =batchsize))
}

search_arxiv <- function(subcat,limit = 10, batchsize = 100){
  cat = paste("cat:",subcat,sep="")
  query = cat
 
  return(arxiv_search(query, limit=limit, batchsize =batchsize))
}

search_arxiv_submission_update_range <- function(subcat,limit = 10, batchsize = 100,
                                                 submittedDateStart, submittedDateEnd,
                                                 updatedStart, updatedEnd){
  submittedRange = paste("submittedDate:[",submittedDateStart," TO ",submittedDateEnd,"]",sep="")
  updatedRange = paste("submittedDate:[",updatedStart," TO ",updatedEnd,"]", sep="")
  cat = paste("cat:",subcat,sep="")
  
  query = paste(cat,submittedRange,updatedRange,sep = " AND ")
  
  return(arxiv_search(query, limit=limit, batchsize =batchsize))
}
