library(rcrossref)

crossref <- function(authors, titles, submission){
  for (i in 1:length(authors)){
    print(paste(i,"------------"))
    
    title = gsub("\r|\n|\r\n", "", titles[i])
    title = gsub(" +", "+",title)
    author = gsub("\\. +|\\|| +","+",authors[i])
    sub_date = strptime(submission[i], format="%Y-%m-%d %H:%M:%S")$year+1900
    
    query_string = paste(title,author,sep="+")
    query_string = gsub("[^a-zA-Z0-9+ -]","",query_string)
    print(query_string)
    
    # Currently filtering a window of +/- 1 year around the submission date.
    r = cr_works(query=query_string,
                 filter=c(from_pub_date=sub_date-1, until_pub_date=sub_date+1), limit=1)
    print(r$meta)
    
    if (i==1){
      results = r$data
    } else {
      results = rbind.fill(r$data,results)
    }  
  }
  
  return(results)
}