#' Function to produce the list of non-reporting countries.
#'
#' @param collection_path String with path where single-year report directories
#'   are stored.
#'
#' @param prefix String with report directory name prefix without trailing
#'   underscore. By default NULL.
#'
#' @return A table with countries/years where the cell is 9 if the country
#'   does not report completely for that year, 1 if it does not report
#'   imports, 2 if it does not report exports.
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
    group_by(area, year) %>%
    mutate(n = n()) %>%
    ungroup() %>%
    mutate(missing = ifelse(n < 2, flow, 9)) %>%
    rename_(non_rep_area = ~ area,
            non_rep_name = ~ name) %>%
    group_by(non_rep_area, non_rep_name, year) %>%
    summarise(missing = max(missing)) %>%
    tidyr::spread(year, missing, fill = '')
}
