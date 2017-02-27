#' Reporting on reporter/flow/hs unique combinations during HS->FCL mapping
#' process.
#'
#' @param uniqhs Data frame with columns reporter, flow and hs.
#' @param tradedataname Character of length 1. Most likely `esdata` or `tldata`.
#'
#' @return Data frame with summary statistics.
#'
#' @import dplyr
#' @export

rprt_uniqhs <- function(uniqhs, tradedataname = NULL) {

  stopifnot(!(is.null(tradedataname)))
  stopifnot(length(tradedataname) == 1L)

  uniq_hs_by_reporter <- uniqhs %>%
    group_by_(~reporter, ~flow) %>%
    summarize_(flow_uniq_hs = ~n()) %>%
    mutate_(flow_uniq_hs = ~ifelse(is.na(flow_uniq_hs), 0, flow_uniq_hs)) %>%
    tidyr::spread_(key_col = "flow", value_col = "flow_uniq_hs") %>%
    rename_(uniq_hs_flow1 = ~`1`, uniq_hs_flow2 = ~`2`) %>%
    mutate_(total_uniq_hs = ~sum(uniq_hs_flow1, uniq_hs_flow2))

  rprt_writetable(uniq_hs_by_reporter, prefix = tradedataname)

  rprt_fulltable(uniq_hs_by_reporter, prefix = tradedataname)
}
