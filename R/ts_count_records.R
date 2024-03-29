#' Count number of records across years.
#'
#' Table with the number of records by reporter/year/flow.
#' It also contains the maximum HS length, the percentage
#' difference in trade flows per year and whether the
#' maximum HS length changed with respect to the previous year.
#'
#' @inheritParams ts_all_reports
#'
#' @import dplyr
#'
ts_count_records <- function(collection_path = NULL, prefix = NULL) {

  elems <- c("esdata_rawdata_nonmrc",
             "tldata_rawdata_nonmrc")

  extract_rprt_elem(collection_path, prefix, elems) %>%
    bind_rows() %>%
    dplyr::select(reporter, name, year, flow, hslength, records_count, nonmrc_hs_prop) %>%
    filter_(~!nonmrc_hs_prop == 1) %>%
    select_(~-nonmrc_hs_prop) %>%
    rename_(rep_code = ~ reporter,
            rep_name = ~ name) %>%
    dplyr::arrange(rep_code, rep_name, flow, year) %>%
    group_by(rep_code, rep_name, flow) %>%
    dplyr::mutate(
      records_diff = records_count / lag(records_count) - 1,
      hs_diff      = hslength != lag(hslength)
    )
}
