library(rcrossref)

crossref <- function(author, title, submission){
  title = gsub("\r|\n|\r\n", "", title)
  title = gsub(" +", "+",title)
  author = gsub("\\. +|\\|| +","+",author)
  sub_date = strptime(submission, format="%Y-%m-%d %H:%M:%S")[['year']]+1900
  
  query_string = paste(title,author,sep="+")
  query_string = gsub("[^a-zA-Z0-9+]","",query_string)
  
  # Currently filtering a window of +/- 1 year around the submission date.
  r = cr_works(query=query_string,
               filter=c(from_pub_date=sub_date-1, until_pub_date=sub_date+1), limit=1)
  
  return(data.frame(r[['data']]))
}