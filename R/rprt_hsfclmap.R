#' Function to produce report on HS->FCL mapping table
#'
#' Function calculates:
#'
#' \itemize{
#'   \item Number of mapping records per country.
#'   \item Length of HS codes.
#'   \item Odd/even digit length of HS codes.
#' }
#'
#' @param maptable Data frame with hsfclmap table.
#' @param year Integer. Reporting year.
#' @return TBD
#' @import dplyr
#' @export
#'
#' @seealso See \url{https://github.com/SWS-Methodology/faoswsTrade/issues/83}
#'   for problem background.

rprt_hsfclmap <- function(maptable, year) {

  stopifnot(!missing(year))

  hsfclmap_by_reporter_stats <- maptable %>%
    group_by_(~area) %>%
    mutate_(totalrecords = ~n()) %>%
    ungroup() %>%
    filter_(~startyear <= year &
              endyear >= year) %>%
    group_by_(~area, ~totalrecords) %>%
    # Using dots and setNames to generate column name with year
    summarize_(.dots = setNames("n()", paste0("records_", year)))

  rprt_writetable(hsfclmap_by_reporter_stats)

  rprt_fulltable(hsfclmap_by_reporter_stats)

}
