#' Reporting on modifications of HS codes in mapping table during HS->FCL mapping
#' process.
#'
#' @param maptable Data frame with columns reporter, flow, maxhslength, fromcodeextchar
#'   and fromcodeext. (produced in mapHS2FCL()).
#' @param tradedataname Character of length 1. Most likely `esdata` or `tldata`.
#'
#' @return Data frame with summary statistics.
#'
#' @export
#' @import dplyr

rprt_map_hschanged <- function(maptable, tradedataname = NULL) {

  stopifnot(!(is.null(tradedataname)))
  stopifnot(length(tradedataname) == 1L)
  stopifnot(all(c("reporter", "flow", "maxhslength",
                   "fromcodeextchar", "fromcodeext") %in%
                  colnames(maptable)))

  map_hschange_all <- maptable %>%
    mutate_(otherlength = ~stringr::str_length(fromcode) != maxhslength)

  map_hschange_all <- add_area_names(map_hschange_all, "fao")

  map_hschange <- map_hschange_all %>%
    filter_(~otherlength) %>%
    select_(~-otherlength)

  rprt_writetable(map_hschange, prefix = tradedataname)

  map_hschange_statistic <- map_hschange_all %>%
    group_by_(~reporter, ~name) %>%
    summarize_(all = ~n(),
               changed = ~sum(otherlength),
               changedprop = ~changed / all)

  rprt_writetable(map_hschange_statistic, prefix = tradedataname)

  map_hschange_statistic <- map_hschange_statistic %>%
    arrange_(~desc(changedprop)) %>%
    mutate_(changedprop = ~ifelse(is.na(changedprop),
                                  as.character(changedprop),
                                  scales::percent(changedprop)))

  rprt_fulltable(map_hschange_statistic, prefix = tradedataname)
}
