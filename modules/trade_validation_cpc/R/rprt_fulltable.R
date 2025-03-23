#' Adds full table to report
#'
#' @param dataset Data frame to report
#' @param level Character of length one. Futile logger level to send messages
#'   to. By default "info".
#' @param prefix Character of length one. Prefix for name to use in the report.
#'   NULL by default.
#' @param pretty_prop Logical of length one. If TRUE all numeric columns with
#'   names end with "_prop" will be converted for easy reading with
#'   scales::percent.
#' @param area_code_class Character "fao", "m49", "geonom" or NULL. If not NULL
#'   columns with area names will be added. NULL by default. See
#'   ?add_area_names for details.
#' @param area_columns Character vector with names of columns containing area
#'   codes. By default c("reporter", "partner").
#'
#' @export
#' @import futile.logger
#' @import dplyr
#' @import scales

rprt_fulltable <- function(dataset,
                           level = "info",
                           prefix = NULL,
                           pretty_prop = TRUE,
                           area_code_class = NULL,
                           area_columns = c("reporter", "partner")) {

  name <- lazyeval::expr_text(dataset)

  if(!is.null(area_code_class)) {

    dataset <- add_area_names(dataset,
                              code_class = area_code_class,
                              area_columns = area_columns)

  }

  if(pretty_prop) {

    # Column names what end with "_prop"
    vars2convert <- stringr::str_subset(colnames(dataset), "_prop$")

    if(length(vars2convert) > 0L) {

      # Column names from previous step what are numeric
      numericvars <- plyr::laply(dataset[vars2convert], is.numeric)

      if(sum(numericvars) > 0L) {
        vars2convert <- vars2convert[numericvars]

        dataset <- dataset %>%
          mutate_at(vars2convert,
                    funs(percent))

        if(missing(pretty_prop)) {
          warning(sprintf(
            "Following columns in dataset %s were\n converted with scales::percent: %s. \nSet pretty_prop = TRUE to avoid this message.",
            name, paste(vars2convert, collapse = ", ")))

        }
      }
    }
  }
  fnc <- paste0("flog.", level)
  flog_capture <- TRUE

  if(!is.null(prefix)) {
    stopifnot(is.character(prefix))
    name <- paste(prefix, name, sep = "_")
  }

  # Supress tibble's table decoration
  dataset <- as.data.frame(dataset, stringsAsFactors = FALSE)

  do.call(fnc, list(msg = name, dataset, capture = flog_capture))
}
