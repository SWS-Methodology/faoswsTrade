#' Extract all unmapped HS codes across years
#'
#' @import dplyr
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
    mutate(fcl = NA_integer_) %>%
    select(year, reporter_fao, reporter, flow, hs_chap, hs, hs_extend, fcl)

}

