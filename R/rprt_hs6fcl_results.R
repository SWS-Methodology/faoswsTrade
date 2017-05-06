#' Reports on results of HS6->FCL mapping process.
#'
#' @import dplyr

rprt_hs6fcl_results <- function(data = NULL, tradedataname = NULL) {

  stopifnot(!is.null(data))
  stopifnot(!is.null(tradedataname))

  data <- add_area_names(data, "fao", "reporter")

  hs6fcl_results_by_reporter <- data %>%
    group_by_(~reporter, ~name, ~flow) %>%
    summarize_(links_total = ~n(),
               mapped_count = ~sum(!is.na(fcl)),
               nonmapped_count = ~sum(is.na(fcl)),
               mapped_prop = ~mapped_count/links_total) %>%
    ungroup()

  rprt_fulltable(hs6fcl_results_by_reporter, prefix = tradedataname,
                 pretty_prop = TRUE)

  rprt_writetable(hs6fcl_results_by_reporter, prefix = tradedataname)

  hs6_links_not_mapped <- data %>%
    filter_(~is.na(fcl))

  hs6_links_mapped <- data %>%
    filter_(~!is.na(fcl))

  rprt_writetable(hs6_links_not_mapped,
                  prefix = tradedataname)
  rprt_writetable(hs6_links_mapped,
                  prefix = tradedataname)

  invisible(data)
}
