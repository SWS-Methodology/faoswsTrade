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
#' @examples ts_reporters("/mnt/storage/sws_share/sas", "complete_tf_cpc")
#'

ts_reporters <- function(collection_path = NULL, prefix = NULL) {

  elems <- c("esdata_rawdata_hslength",
             "tldata_rawdata_hslength")

  extract_rprt_elem(collection_path, prefix, elems) %>%
    bind_rows() %>%
    ungroup() %>%
    select(year, reporter, name, year) %>%
    distinct() %>%
    mutate(exist = 1) %>%
    arrange(year, name) %>%
    rename_(rep_code = ~ reporter,
            rep_name = ~ name) %>%
    tidyr::spread(year, exist, fill = '')
}
