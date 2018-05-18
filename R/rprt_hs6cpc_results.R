#' Reports on results of HS6->CPC mapping process.
#'
#' @param data Data.
#' @param tradedataname Trade data name.
#'
#' @import dplyr

rprt_hs6cpc_results <- function(data = NULL, tradedataname = NULL) {

  stopifnot(!is.null(data))
  stopifnot(!is.null(tradedataname))

  data <- add_area_names(data, "fao", "reporter")

  hs6cpc_results_by_reporter <- data %>%
    group_by_(~reporter, ~name, ~flow) %>%
    summarize_(links_total     = ~n(),
               mapped_count    = ~sum(!is.na(cpc)),
               nonmapped_count = ~sum(is.na(cpc)),
               mapped_prop     = ~mapped_count/links_total) %>%
    ungroup()

  rprt_fulltable(hs6cpc_results_by_reporter, prefix = tradedataname)

  rprt_writetable(hs6cpc_results_by_reporter, prefix = tradedataname)

  hs6_links_mapped <- data %>%
    filter_(~!is.na(cpc))

  rprt_writetable(hs6_links_mapped, prefix = tradedataname)

  invisible(data)
}

