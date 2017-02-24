#' Writes full table for report collection
#'
#' @param dataset  
#' @export

rprt_writetable <- function(dataset) {
  
  stopifnot(exists(reportdir))
  collectreports <- exists(rprt_data)
  
  name <- substitute(dataset)
  
  write.csv(dataset, 
            file = file.path(reportdir, paste0(name, ".csv")))
  
  if(collectreports) rprt_data[[name]] <<- dataset
  
  invisible(dataset)
  
}