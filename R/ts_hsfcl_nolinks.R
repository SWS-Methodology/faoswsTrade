#' Extract all unmapped HS codes across years
#' @inheritParams ts_all_reports
#'
#'@import dplyr
#'
#'@examples
#' library(ggplot2)
#' library(dplyr)
#'
#' uniq_hs <- ts_hsfcl_nolinks("/mnt/storage/sws_share/sas",
#'                             "complete_tf_cpc")
#' uniq_hs %>%
#'   group_by(year, flow) %>%
#'   summarize(uniq_hs = n()) %>%
#'   ggplot(aes(as.factor(year), uniq_hs)) +
#'   geom_bar(stat = "identity") +
#'   ylab("Number of unique unmapped HS") + xlab("") +
#'   ggtitle(paste0("There are ",
#'                  nrow(uniq_hs),
#'                  " unmapped HS codes"),
#'           "New reporters absent in the map are not included")
#'
ts_hsfcl_nolinks <- function(collection_path = NULL, prefix = NULL) {

  elems <- c("esdata_hsfcl_nolinks",
             "tldata_hsfcl_nolinks")

  extract_rprt_elem(collection_path, prefix, elems) %>%
    bind_rows() %>%
    ungroup() %>%
    select(reporter_fao, reporter, flow, hs = hs_orig, hs_extend, year) %>%
    group_by(reporter, flow, hs) %>%
    mutate(age = rank(year)) %>%
    ungroup() %>%
    filter(age == 1) %>%
    select(-age) %>%
    mutate(hs_chap = stringr::str_sub(hs, end = 2L)) %>%
    arrange(year, reporter, flow, hs_chap, hs) %>%
    mutate(fcl = "", mapped_by = "", details = "", tl_description = "") %>%
    select(mapped_by, year, reporter_fao, reporter_name = reporter,
           flow, hs_chap, hs, hs_extend, fcl, details, tl_description)
}

