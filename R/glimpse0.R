#' Drops dplyr::glimpse's invisible value
#'
#' It is required to include glimpse's output into
#' logging messages of futile.logger.
#'
#' @export

glimpse0 <- function(tbl) {

  writeLines(capture.output(dplyr::glimpse(tbl)))

}
