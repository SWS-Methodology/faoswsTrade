#' Reporting on reporter/flow/hs unique combinations during HS->FCL mapping
#' process.
#'
#' @param hslength Data frame with columns reporter, flow and maxhslength
#'   (produced by maxHSLength().
#' @param tradedataname Character of length 1. Most likely `esdata` or `tldata`.
#'
#' @return Data frame with summary statistics. Currently hslength itself.
#'
#' @export

rprt_hslength <- function(hslength, tradedataname = NULL) {

  stopifnot(!(is.null(tradedataname)))
  stopifnot(length(tradedataname) == 1L)

  rprt_writetable(hslength, prefix = tradedataname)

  rprt_fulltable(hslength, prefix = tradedataname)
}
