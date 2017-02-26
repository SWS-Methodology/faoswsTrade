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

  hsfclmap_by_reporter_stats <- maptable %>%
    group_by_(~area) %>%
    summarise_(count = ~n())

  rprt_writetable(hsfclmap_by_reporter_stats)

  rprt_report(hsfclmap_by_reporter_stats)

}
