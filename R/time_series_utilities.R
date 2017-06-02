#' Provides list of directories with names matching reporting scheme.
#' @param collection_path String with path where report directories are stored.
#' @param prefix String with report directory name prefix without trailing
#'   underscore. By default NULL.
#' @return Character vector with full names of suitable directories.
#'

list_rprt_dirs <- function(collection_path = NULL, prefix = NULL) {
  stopifnot(!is.null(collection_path))
  stopifnot(!is.null(prefix))
  stopifnot(dir.exists(collection_path))

  prefix <- paste0(prefix, "_\\d{4}_")

  stringr::str_subset(list.dirs(collection_path,
                                full.names = FALSE,
                                recursive = FALSE), prefix)

}

#' Extracts year from report directory name
#' @param dirname Character vector with directories' names. NULL by default.
#' @param prefix A string with report directory name prefix (without trailing
#'   underscore. By default NULL.
#' @return Integer vector same length as `dirname` with years.
#' @import dplyr

extract_rprt_year <- function(dirname = NULL, prefix = NULL) {
  stopifnot(!is.null(dirname))
  stopifnot(!is.null(prefix))
  stopifnot(length(prefix) == 1L)

  year <- stringr::str_match_all(dirname, paste0(prefix, "_(\\d{4})")) %>%
    vapply(function(x) as.integer(x[[2]]), integer(1L))

  # NA in years
  stopifnot(!any(is.na(year)))

  # Duplicates in years
  stopifnot(!all(duplicated(year)))

  # Missing years
  stopifnot(length(year) == length(seq.int(from = min(year),
                                           to = max(year),
                                           by = 1L)))

  year
}

#' Extract an element from list stored in RDS archive in report directory.
#' @param collection_path String with path where report directories are stored.
#' @param prefix A string with report directory name prefix (without trailing
#'   underscore. By default NULL.
#' @param elem Character vector with names of elements to extract.
#' @return List of extracted tables
#'

extract_rprt_elem <- function(collection_path = NULL,
                              prefix = NULL,
                              elem = NULL) {
  stopifnot(!is.null(dirname))
  stopifnot(!is.null(prefix))
  stopifnot(!is.null(elem))
  stopifnot(length(prefix) == 1L)

  if (length(elem) > 1L) {
    return(lapply(elem,
                  extract_rprt_elem,
                  collection_path = collection_path,
                  prefix = prefix
                  ))
  }

  rprt_dirs <- list_rprt_dirs(collection_path, prefix)
  rprt_years <- extract_rprt_year(rprt_dirs, prefix)

  elems_list <- mapply(function(dir, year) {
    rprt_file_path <- file.path(collection_path,
                                basename(dir),
                                "datadir",
                                paste0(elem, ".rds"))
    stopifnot(file.exists(rprt_file_path))
    rprt_data <- readRDS(rprt_file_path)
    stopifnot("data.frame" %in% class(rprt_data))
    if (!"year" %in% colnames(rprt_data)) {
      if (nrow(rprt_data) > 0L) {
        rprt_data$year <- year
      } else {
        rprt_data$year <- integer(0L)
      }
    }
    rprt_data
  }, dir = rprt_dirs, year = rprt_years, SIMPLIFY = FALSE)

  dplyr::bind_rows(elems_list)
}

#' Writes table with time series report to specific directory as csv file.
#'
#' @param rprt Data frame to save.
#' @param ts_reports_path Directory path to save csv file.
#'
ts_write_rprt <- function(rprt = NULL,
                          ts_reports_path = NULL,
                          rprt_name = NULL) {

  stopifnot(!is.null(rprt))
  stopifnot(!is.null(ts_reports_path))
  stopifnot(length(rprt) == 1L)

  if (basename(ts_reports_path) != "ts_reports")
    ts_reports_path <- file.path(ts_reports_path, "ts_reports")

  if (!dir.exists(ts_reports_path))
    dir.create(ts_reports_path,
               showWarnings = FALSE,
               recursive = TRUE)

  write.table(rprt,
              file = file.path(ts_reports_path,
                               paste0(rprt_name, ".csv")),
              sep = ",",
              row.names = FALSE)

  invisible(NULL)

}
