#' Reporting on results of HS ranges matches by function hsInRange.
#'
#' @param uniqhs Data frame with columns reporter, flow, hsext and fcl.
#'   (produced in mapHS2FCL()).
#' @param tradedataname Character of length 1. Most likely `esdata` or `tldata`.
#'
#' @return Data frame with summary statistics.
#'
#' @export
#' @import dplyr

rprt_hsinrange <- function(uniqhs, tradedataname = NULL) {

  stopifnot(!(is.null(tradedataname)))
  stopifnot(length(tradedataname) == 1L)
  stopifnot(all(c("reporter", "flow", "hsext") %in%
                  colnames(uniqhs)))

  rprt_writetable(uniqhs, prefix = tradedataname)

  # rprt_fulltable(uniqhs, prefix = tradedataname)
}
