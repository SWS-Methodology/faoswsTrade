#' Function to produce the list of non-reporting countries.
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
#' @examples ts_non_reporters("/mnt/storage/sws_share/sas", "complete_tf_cpc")
#'

ts_non_reporters <- function(collection_path = NULL, prefix = NULL) {

  elems <- c("flows_to_mirror_raw")

  extract_rprt_elem(collection_path, prefix, elems) %>%
    bind_rows() %>%
    ungroup() %>%
    select(area, flow, name, year) %>%
    mutate(exist = 1) %>%
    arrange(year, name, flow) %>%
    tidyr::spread(year, exist)

}
