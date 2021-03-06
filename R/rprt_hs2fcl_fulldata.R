#' Reporting on multiple matches during HS->FCL matching by hsInRange function.
#'
#' The function generates the following statistics per reporter, hs6,
#' reporter/hs6:
#' \itemize{
#'   \item Number of nonmatched records.
#'   \item Proportion of nonmatched records.
#'   \item Total value (cost) of nonmatched records.
#'   \item Proportion of total value of nonmatched records.}
#'
#' @param tradedata Trade data set with fcl column (after mapping process).
#' @param tradedataname Character of length 1. Most likely `esdata` or `tldata`.
#'
#' @return Data frame with summary statistics.
#'
#' @import dplyr

rprt_hs2fcl_fulldata <- function(tradedata, tradedataname = NULL) {

  stopifnot(!(is.null(tradedataname)))
  stopifnot(length(tradedataname) == 1L)

  tradedataname <- paste0(tradedataname, "_fulldata")

  if(!"nolink" %in% colnames(tradedata))
    tradedata$nolink <- is.na(tradedata$fcl) else
      stopifnot(is.logical(tradedata$nolink))

  if(!"hs6" %in% colnames(tradedata))
    tradedata$hs6 <- stringr::str_sub(tradedata$hs, end = 6L)

  stats <- list(nolink_count      = ~sum(nolink),
                nolink_prop       = ~nolink_count / n(),
                nolink_value      = ~sum(value * as.integer(nolink)),
                nolink_value_prop = ~nolink_value / sum(value))

  filters <- list(~nolink_count != 0L)
  sorting <- list(~dplyr::desc(nolink_prop))

  nolinks_total <- tradedata %>%
    summarise_(.dots = stats)

  nolinks_byreporter <- tradedata %>%
    group_by_(~reporter) %>%
    summarise_(.dots = stats) %>%
    filter_(.dots = filters) %>%
    arrange_(.dots = sorting)

  nolinks_byreporterhs6 <- tradedata %>%
    group_by_(~reporter, ~hs6) %>%
    summarize_(.dots = stats) %>%
    ungroup() %>%
    filter_(.dots = filters) %>%
    arrange_(.dots = sorting)

  nolinks_byhs6 <- tradedata %>%
    group_by_(~hs6) %>%
    summarize_(.dots = stats) %>%
    filter_(.dots = filters) %>%
    arrange_(.dots = sorting)

  nolinks_byreporterhs6 <- add_area_names(nolinks_byreporterhs6, "fao")
  nolinks_byreporter <- add_area_names(nolinks_byreporter, "fao")

  rprt_writetable(nolinks_total,         prefix = tradedataname)
  rprt_writetable(nolinks_byreporter,    prefix = tradedataname)
  rprt_writetable(nolinks_byreporterhs6, prefix = tradedataname)
  rprt_writetable(nolinks_byhs6,         prefix = tradedataname)

  rprt_fulltable(nolinks_total)
  rprt_fulltable(nolinks_byreporter)
  rprt_fulltable(nolinks_byreporterhs6)
  rprt_fulltable(nolinks_byhs6)


  # All records with failed mapping
  hsfcl_nolinks <- tradedata %>%
    filter_(~is.na(fcl)) %>%
    mutate_(year = year) %>%
    select_(~year, ~reporter, ~partner, ~flow, ~hs,
            ~fcl, ~value, ~weight, ~qty)

  rprt_writetable(hsfcl_nolinks, prefix = tradedataname)

  # Canada case (issue #78)
  if (33 %in% tradedata$reporter) {

    canada_links <- tradedata %>%
      filter_(~reporter == 33) %>%
      mutate_(year = year) %>%
      select_(~year, ~reporter, ~flow, ~hs,
              ~fcl) %>%
      distinct()

    rprt_writetable(canada_links, prefix = tradedataname)

  }
}
