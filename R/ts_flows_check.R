#' Check if value and quantity and weight exist.
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
#' @examples ts_content_check("/mnt/storage/sws_share/sas", "complete_tf_cpc")
#'

ts_flows_check <- function(collection_path = NULL, prefix = NULL) {

  elems <- c("esdata_rawdata_nonmrc",
             "tldata_rawdata_nonmrc")

  extract_rprt_elem(collection_path, prefix, elems) %>%
    bind_rows() %>%
    ungroup() %>%
    select(year, reporter, name, flow, noqty_prop, novalue_prop) %>%
    mutate(qty = 1*(noqty_prop == 1), value = 1*(novalue_prop == 1)) %>%
    select(-noqty_prop, -novalue_prop) %>%
    rename_(rep_code = ~ reporter,
            rep_name = ~ name)
}
