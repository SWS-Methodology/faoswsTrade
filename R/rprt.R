#' Function to call reporting instructions
#'
#' We want the trade module to run even one of reporting functions fails, so
#' every call of any reporting function should be wrapped by try(). Also we do
#' not want to export too many functions in the user space. So this function
#' could be the one from reporting functions to be exported.
#'
#' @param data Data set to make report on.
#' @param report Name of reporting function to call as string. Prefix "rprt_"
#'   can be omitted.
#' @param ... Additional arguments passed onto report function.
#'
#' @return Original data invisibly.
#'
#' @import futile.logger
#' @export

rprt <- function(data = NULL, report = NULL, ...) {

  stopifnot(!is.null(data))
  stopifnot(!is.null(report))
  stopifnot(length(report) == 1L)

  report <- ifelse(grepl("^rprt_", report),
                   report, paste0("rprt_", report))

  report_try <- try(do.call(report, list(data, ...)), silent = TRUE)

  if(inherits(report_try, "try-error")) {
    err_text <- as.character(report_try)
    flog.error(paste0("Reporting function ", report,
                      " failed with error ", err_text),
               name = "dev")
  }

  invisible(data)

}
