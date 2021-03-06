#' Function to produce the list of reporters cointained in the file.
#'
#' @param collection_path String with path where single-year report directories
#'   are stored.
#'
#' @param prefix String with report directory name prefix without trailing
#'   underscore. By default NULL.
#'
#' @return NULL invisibly.
#' @export
#' @import dplyr
#' @examples
#' \dontrun{
#' ts_reporters("/mnt/storage/sws_share/sas", "complete_tf_cpc")
#' }
#'

ts_reporters <- function(collection_path = NULL, prefix = NULL) {

  elems <- c("esdata_rawdata_hslength",
             "tldata_rep_table")

  extract_rprt_elem(collection_path, prefix, elems) %>%
    lapply(function(x) x %>% dplyr::mutate(reporter = as.character(reporter))) %>%
    bind_rows() %>%
    ungroup() %>%
    dplyr::select(year, reporter, name) %>%
    distinct() %>%
    dplyr::mutate(exist = 1) %>%
    dplyr::arrange(year, name) %>%
    rename_(rep_code = ~ reporter, rep_name = ~ name) %>%
    tidyr::spread(year, exist, fill = '')
}
