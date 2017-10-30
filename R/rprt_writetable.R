#' Writes full table for report collection. Table is saved as csv file and added
#' to rprt_data list.
#'
#' @param dataset Data frame to save
#' @param prefix Optional string to be added to the name of output csv-file and
#'   table in rprt_data list. NULL by default.
#' @param subdir Optional string specifying name of subdirectory in the report
#'   directory
#' @export
#' @return Original data set invisibly.

rprt_writetable <- function(dataset, prefix = NULL, subdir = NULL) {

  stopifnot(exists("reportdir"))
  stopifnot(file.exists(reportdir))

  # Dir for RDS files
  datadir <- normalizePath(file.path(reportdir, "datadir"), mustWork = FALSE)
  if (!file.exists(datadir)) dir.create(datadir)

  # Dir for detailed csv
  if (!is.null(subdir)) {
    reportdir <- normalizePath(file.path(reportdir, subdir), mustWork = FALSE)
    if (!file.exists(reportdir)) dir.create(reportdir)
  }


  # We want name to be the same as name of original variable
  # in main.R code
  name <- lazyeval::expr_text(dataset)

  if (!is.null(prefix)) {
    stopifnot(is.character(prefix))
    stopifnot(length(prefix) == 1L)
    name <- paste(prefix, name, sep = "_")
  }

  saveFileForPPR(data = dataset,
                 file = file.path(reportdir, paste0(name, ".csv")),
                 type = "csv")

  saveFileForPPR(data = dataset,
                 file = file.path(datadir, paste0(name, ".rds")),
                 type = "rds")

  invisible(dataset)

}
