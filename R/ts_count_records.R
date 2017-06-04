#' Count number of records across years.
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
    select(reporter, name, year, flow, hslength, records_count)
}
