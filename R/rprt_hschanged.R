#' Reporting on HS code changes in trade data during HS->FCL mapping
#' process.
#'
#' @param uniqhs Data frame with columns reporter, flow, maxhslength, hsextchar
#'   and hsext. (produced in mapHS2FCL()).
#' @param tradedataname Character of length 1. Most likely `esdata` or `tldata`.
#'
#' @return Data frame with summary statistics.
#'
#' @export
#' @import dplyr

rprt_hschanged <- function(uniqhs, tradedataname = NULL) {

  stopifnot(!(is.null(tradedataname)))
  stopifnot(length(tradedataname) == 1L)
  stopifnot(all(c("reporter", "flow", "maxhslength",
                   "hsextchar", "hsext") %in%
                  colnames(uniqhs)))

  hschange_all <- uniqhs %>%
    mutate_(otherlength = ~stringr::str_length(hs) != maxhslength)

  hschange_all <- add_area_names(hschange_all, "fao")

  hschange <- hschange_all %>%
    filter_(~otherlength) %>%
    select_(~-otherlength)

  rprt_writetable(hschange, prefix = tradedataname, subdir = "details")

  hschange_statistic <- hschange_all %>%
    group_by_(~reporter, ~name) %>%
    summarize_(all = ~n(),
               changed = ~sum(otherlength),
               changedprop = ~changed / all)

  rprt_writetable(hschange_statistic, prefix = tradedataname,
                  subdir = "details")

  hschange_statistic <- hschange_statistic %>%
    arrange_(~dplyr::desc(changedprop)) %>%
    mutate_(changedprop = ~ifelse(is.na(changedprop),
                                  as.character(changedprop),
                                  scales::percent(changedprop)))

  rprt_fulltable(hschange_statistic, prefix = tradedataname)
}
