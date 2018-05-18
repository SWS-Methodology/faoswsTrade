#' Function to produce report on HS->CPC mapping table
#'
#' Function calculates:
#'
#' \itemize{
#'   \item Number of mapping records per country.
#'   \item Length of HS codes.
#'   \item Odd/even digit length of HS codes.
#' }
#'
#' @param maptable Data frame with hscpcmap table.
#' @param year Integer. Reporting year.
#' @return TBD
#' @import dplyr
#'
#' @seealso See \url{https://github.com/SWS-Methodology/faoswsTrade/issues/83}
#'   for problem background.

rprt_hscpcmap <- function(maptable, year) {

  stopifnot(!missing(year))

  hscpcmap_by_reporter_stats <- maptable %>%
    group_by_(~area) %>%
    mutate_(totalrecords = ~n(),
            hslength     = ~stringr::str_length(fromcode),
            maxhslength  = ~max(hslength),
            minhslength  = ~min(hslength)) %>%
    ungroup() %>%
    filter_(~startyear <= year & endyear >= year) %>%
    group_by_(~area, ~totalrecords, ~maxhslength, ~minhslength) %>%
    # Using dots and setNames to generate column name with year
    summarize_(.dots = setNames(c(
      "n()"), c(
        paste0("records_", year)))) %>%
    mutate_(.dots = setNames(paste0("records_", year, "/ totalrecords"),
                             paste0("prop_", year))) %>%
    select(1, 2, 5, 6, 3, 4) %>%
    ungroup()

  hscpcmap_by_reporter_stats <- add_area_names(hscpcmap_by_reporter_stats,
                                               "fao", "area")

  rprt_writetable(hscpcmap_by_reporter_stats, subdir = "details")

  # In the report we pretty percents instead of decimals
  hscpcmap_by_reporter_stats <- hscpcmap_by_reporter_stats %>%
    mutate_(.dots = setNames(paste0("scales::percent(prop_", year, ")"),
                             paste0("prop_", year)))

  rprt_fulltable(hscpcmap_by_reporter_stats)

}

