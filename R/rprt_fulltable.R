#' Adds full table to report
#'
#' @param dataset Data frame to report
#'
#' @export
#' @import futile.logger
#' @import dplyr

rprt_fulltable <- function(dataset, level = "info") {

  fnc <- paste0("flog.", level)
  flog_capture <- TRUE
  name <- lazyeval::expr_text(dataset)

  # Supress tibble's table decoration
  dataset <- as.data.frame(dataset, stringsAsFactors = FALSE)

  do.call(fnc, list(msg = name, dataset, capture = flog_capture))
}
