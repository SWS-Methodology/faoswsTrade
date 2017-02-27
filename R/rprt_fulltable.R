#' Adds full table to report
#'
#' @param dataset Data frame to report
#'
#' @export
#' @import futile.logger

rprt_fulltable <- function(dataset, level = "info", prefix = NULL) {

  fnc <- paste0("flog.", level)
  flog_capture <- TRUE
  name <- lazyeval::expr_text(dataset)
  if(!is.null(prefix)) {
    stopifnot(is.character(prefix))
    name <- paste(prefix, name, sep = "_")
  }

  # Supress tibble's table decoration
  dataset <- as.data.frame(dataset, stringsAsFactors = FALSE)

  do.call(fnc, list(msg = name, dataset, capture = flog_capture))
}
