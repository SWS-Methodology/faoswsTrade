#' Adds info to futile.logger report
#'
#' @param msg Object to add
#' @param level Character. Futile logger level to use. INFO by default.
#'
#' @import futile.logger
#' @export
#'

rprt_report <- function(msg, level = "info") {

  fnc <- paste0("flog.", level)

  # We want pretty format for tables in log messages
  if(any(class(msg) %in% c("tbl_df",  "tbl", "data.frame", "data.table")))
    return(do.call(fnc, list(msg = rprt_glimpse0(msg))))

  do.call(fnc, list(msg = msg))

}
