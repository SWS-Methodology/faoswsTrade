#' Drops dplyr::glimpse's invisible value
#'
#' It is required to include glimpse's output into
#' logging messages of futile.logger.
#'
#' @export

rprt_glimpse0 <- function(tbl) {

  writeLines(capture.output(dplyr::glimpse(tbl)))

}
