#' Writes full table for report collection
#'
#' @param dataset  
#' @export

rprt_writetable <- function(dataset) {
  
  stopifnot(exists(reportdir))
  stopifnot(exists(rprt_data))
  stopifnot(is.list(rprt_data))
  
  name <- substitute(dataset)
  
  write.csv(dataset, 
            file = file.path(reportdir, paste0(name, ".csv")))
  
  rprt_data[[name]] <<- dataset
  
  invisible(dataset)
  
}