#' Reporting on multiple matches during HS->CPC matching by hsInRange function.
#'
#' @param uniqhs Data frame with columns reporter, flow, hsext and cpc
#'   (produced in mapHS2CPC()).
#' @param tradedataname Character of length 1. Most likely `esdata` or `tldata`.
#'
#' @return Data frame with summary statistics.
#'
#' @export
#' @import dplyr

rprt_hs2cpc_multilinks <- function(uniqhs, tradedataname = NULL) {

  stopifnot(!(is.null(tradedataname)))
  stopifnot(length(tradedataname) == 1L)
  stopifnot(all(c("reporter", "flow", "hsext", "datumid") %in%
                  colnames(uniqhs)))

  hscpc_matchescount <- uniqhs %>%
    group_by_(~datumid) %>%
    mutate_(matches = ~n()) %>%
    ungroup() %>%
    mutate_(multilink = ~matches > 1L)

  hscpc_matchescount <- add_area_names(hscpc_matchescount, "fao")

  hscpc_multilinks <- hscpc_matchescount %>%
    filter_(~multilink) %>%
    select_(~-multilink)

  rprt_writetable(hscpc_multilinks, prefix = tradedataname)

  hscpc_multilinks_statistic <- hscpc_matchescount %>%
    group_by_(~reporter, ~name) %>%
    summarise_(multicount = ~sum(multilink),
               multiprop  = ~multicount / n()) %>%
    ungroup() %>%
    filter_(~multicount > 0L)

  rprt_writetable(hscpc_multilinks_statistic, prefix = tradedataname)

  hscpc_multilinks_statistic <- hscpc_multilinks_statistic %>%
    arrange_(~dplyr::desc(multiprop)) %>%
    mutate_(multiprop = ~scales::percent(multiprop))

  rprt_fulltable(hscpc_multilinks_statistic, prefix = tradedataname)
}

