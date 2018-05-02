#' Check if quantity and weight both exist.
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
#' ts_content_check("/mnt/storage/sws_share/sas", "complete_tf_cpc")
#' }
#'

ts_content_check <- function(collection_path = NULL, prefix = NULL) {

  elems <- c("esdata_rawdata_nonmrc",
             "tldata_rawdata_nonmrc")

  extract_rprt_elem(collection_path, prefix, elems) %>%
    bind_rows() %>%
    ungroup() %>%
    select(year, reporter, name, flow, nonmrc_hs_prop) %>%
    filter_(~!nonmrc_hs_prop == 1) %>%
    select_(~-nonmrc_hs_prop) %>%
    dplyr::mutate(
      flow = recode(
               flow,
               `1` = 'imports',
               `2` = 'exports',
               `3` = 're_exports',
               `4` = 're_imports'
             )
      ) %>%
    rename_(rep_code = ~ reporter,
            rep_name = ~ name) %>%
    dplyr::mutate(i=1) %>% tidyr::spread(flow, i, fill = "")
}
