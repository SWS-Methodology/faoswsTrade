#' Count number of records across years.
#'
#' Table with the number of records by reporter/year/flow.
#' It also contains the maximum HS length, the percentage
#' difference in trade flows per year and whether the
#' maximum HS length changed with respect to the previous year.
#'
#' @inherit Params ts_all_reports
#'
#' @import dplyr
#'
ts_count_records <- function(collection_path = NULL, prefix = NULL) {

  elems <- c("esdata_rawdata_nonmrc",
             "tldata_rawdata_nonmrc")

  extract_rprt_elem(collection_path, prefix, elems) %>%
    bind_rows() %>%
    select(reporter, name, year, flow, hslength, records_count) %>%
    rename_(rep_code = ~ reporter,
            rep_name = ~ name) %>%
    arrange(rep_code, rep_name, flow, year) %>%
    group_by(rep_code, rep_name, flow) %>%
    mutate(
      records_diff = records_count / lag(records_count) - 1,
      hs_diff = hslength != lag(hslength)
    )
}
