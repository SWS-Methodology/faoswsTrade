#' Extracts time series data from preprocessing reports
#'
#' @inherit Params ts_all_reports
#' @import dplyr
#'
ts_preproc <- function(collection_path = NULL, prefix = NULL) {

  elems <- c("esdata_rawdata_nonmrc",
             "tldata_rawdata_nonmrc")

  extract_rprt_elem(collection_path, prefix, elems) %>%
    bind_rows() %>%
    rename_(rep_code = ~ reporter,
            rep_name = ~ name)

}
