#' Extracts time series of not mapped HS codes number
#' @inheritParams ts_all_reports
#' @import dplyr
#' @return Data frame
#' @examples
#' \dontrun{
#' library(ggplot2)
#' library(dygraph)
#' library(dplyr)
#'
#' ts1 <- ts_hsfcl_nolinks_statistic("/mnt/storage/sws_share/chr", "complete_tf_cpc")
#'
#' ts1 %>%
#'   group_by(year) %>%
#'   dplyr::summarize(total_nolinks = sum(nolinks)) %>%
#'   ungroup() %>%
#'   ggplot(aes(year, total_nolinks)) +
#'   geom_bar(stat = "identity")
#'
#' ts1 %>%
#'   group_by(year) %>%
#'   dplyr::summarize(total_nolinks = sum(nolinks)) %>%
#'   ungroup() %>%
#'   dygraph(
#'     ylab = "Total number of unmapped HS (duplicates across years possible)",
#'     main = "Unmapped HS codes (Tariff line and Eurostat)")
#' }


ts_hsfcl_nolinks_statistic <- function(collection_path = NULL, prefix = NULL) {

  elems <- c("esdata_hsfcl_nolinks_statistic",
             "tldata_hsfcl_nolinks_statistic")

  extract_rprt_elem(collection_path, prefix, elems) %>%
    bind_rows() %>%
    ungroup()

}
