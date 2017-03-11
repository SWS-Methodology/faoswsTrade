#' Writes full table for report collection
#'
#' @param dataset Data frame to save
#' @export

rprt_writetable <- function(dataset, prefix = NULL) {

  stopifnot(exists("reportdir"))
  collectreports <- exists("rprt_data")

  # We want name to be the same as name of original variable
  # in main.R code
  name <- lazyeval::expr_text(dataset)

  if(!is.null(prefix)) {
    stopifnot(is.character(prefix))
    name <- paste(prefix, name, sep = "_")
  }

  write.table(dataset,
            file = file.path(reportdir, paste0(name, ".csv")),
            sep = ",",
            row.names = FALSE)

  if(collectreports) rprt_data[[name]] <<- dataset

  invisible(dataset)

}