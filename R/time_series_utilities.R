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

  stopifnot(!any(is.na(year)))

  year
}

#' Extract an element from list stored in RDS archive in report directory.
#' @param collection_path String with path where report directories are stored.
#' @param prefix A string with report directory name prefix (without trailing
#'   underscore. By default NULL.
#' @param elem Character vector with names of elements to extract.
#' @param rprt_file_name String with name of RDS file with reports.
#'   `report_data.rds` by defaul.`
#' @return
#'

extract_rprt_elem <- function(collection_path = NULL,
                              prefix = NULL,
                              elem = NULL,
                              rprt_file_name = "report_data.rds") {
  stopifnot(!is.null(dirname))
  stopifnot(!is.null(prefix))
  stopifnot(!is.null(elem))
  stopifnot(length(prefix) == 1L)

  rprt_dirs <- list_rprt_dirs(collection_path, prefix)
  rprt_years <- extract_rprt_year(rprt_dirs, prefix)

  elems_list <- mapply(function(dir, year) {
    rprt_file_path <- file.path(collection_path, basename(dir), rprt_file_name)
    stopifnot(file.exists(rprt_file_path))
    rprt_data <- readRDS(rprt_file_path)
    stopifnot(is.list(rprt_data))
    stopifnot(all(elem %in% names(rprt_data)))
    rprt_data <- rprt_data[elem]
    rprt_data[["year"]] <- year
    rprt_data
  }, dir = rprt_dirs, year = rprt_years, SIMPLIFY = FALSE)

}


#' Bind similar data frames from different years
#' @param rprt_elems_list List produced by `extract_rprt_elem`.
#' @return Data frame if there is one report, and list of frames if more.

bind_rprts <- function(rprt_elems_list) {
  elem_names <- names(rprt_elems_list[[1]])
  stopifnot(all(vapply(rprt_elems_list,
                       function(x) identical(elem_names, names(x)),
                       logical(1L))))

  elem_names <- elem_names[elem_names != "year"]

  all_years_rpts_list <- lapply(elem_names, function(nm) {
    dplyr::bind_rows(lapply(rprt_elems_list,
                            function(one_year_rprt_list) {
                              year <- one_year_rprt_list$year
                              elem <- one_year_rprt_list[[nm]]
                              stopifnot("data.frame" %in% class(elem))
                              if (!"year" %in% colnames(elem)) {
                                elem$year <- year
                              }
                              elem
                              }))

  })

  names(all_years_rpts_list) <- elem_names
  all_years_rpts_list

}

