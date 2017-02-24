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

  do.call(fnc, list(msg = msg))

}
