#' Writes full table for report collection
#'
#' @param dataset
#' @export

rprt_writetable <- function(dataset) {

  stopifnot(exists("reportdir"))
  collectreports <- exists("rprt_data")

  name <- lazyeval::expr_text(dataset)

  write.table(dataset,
            file = file.path(reportdir, paste0(name, ".csv")),
            row.names = FALSE)

  if(collectreports) rprt_data[[name]] <<- dataset

  invisible(dataset)

}
