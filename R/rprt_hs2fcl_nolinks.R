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

  hsfcl_nolinks <- uniqhs %>%
    filter_(~is.na(fcl))

  rprt_writetable(hsfcl_nolinks, prefix = tradedataname)

  hsfcl_nolinks_statistic <- uniqhs %>%
    mutate_(nolink = ~is.na(fcl)) %>%
    group_by_(~reporter, ~flow) %>%
    summarize_(nolinks = ~sum(nolink),
               nolinks_prop = ~nolinks / n()) %>%
    filter_(~nolinks > 0L)

  rprt_writetable(hsfcl_nolinks_statistic, prefix = tradedataname)

  hsfcl_nolinks_statistic <- hsfcl_nolinks_statistic %>%
    group_by_(~flow) %>%
    arrange_(~desc(nolinks)) %>%
    mutate_(nolinks_prop = ~scales::percent(nolinks_prop)) %>%
    ungroup()

  rprt_fulltable(hsfcl_nolinks_statistic, prefix = tradedataname)
}
