#' Reporting on multiple matches during HS->FCL matching by hsInRange function.
#'
#' @param uniqhs Data frame with columns reporter, flow, hsext and fcl.
#'   (produced in mapHS2FCL()).
#' @param tradedataname Character of length 1. Most likely `esdata` or `tldata`.
#'
#' @return Data frame with summary statistics.
#'
#' @export
#' @import dplyr

rprt_hs2fcl_multilinks <- function(uniqhs, tradedataname = NULL) {

  stopifnot(!(is.null(tradedataname)))
  stopifnot(length(tradedataname) == 1L)
  stopifnot(all(c("reporter", "flow", "hsext", "datumid") %in%
                  colnames(uniqhs)))

  hsfcl_matchescount <- uniqhs %>%
    group_by_(~datumid) %>%
    mutate_(matches = ~n()) %>%
    ungroup() %>%
    mutate_(multilink = ~matches > 1L)

  hsfcl_multilinks <- hsfcl_matchescount %>%
    filter_(~multilink) %>%
    select_(~-multilink)

  rprt_writetable(hsfcl_multilinks, prefix = tradedataname)

  hsfcl_multilinks_statistic <- hsfcl_matchescount %>%
    group_by_(~reporter) %>%
    summarise_(multicount = ~sum(multilink),
               multiprop  = ~multicount / n()) %>%
    ungroup() %>%
    filter_(~multicount > 0L)

  rprt_writetable(hsfcl_multilinks_statistic, prefix = tradedataname)

  hsfcl_multilinks_statistic <- hsfcl_multilinks_statistic %>%
    arrange_(~desc(multiprop)) %>%
    mutate_(multiprop = ~scales::percent(multiprop))

  rprt_fulltable(hsfcl_multilinks_statistic, prefix = tradedataname)
}
