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
#' @examples ts_reporters("/mnt/storage/sws_share/sas", "esdata_hs2fcl_mapped_links")
#'



ts_reporters = function(collection_path = NULL, prefix = NULL) {

  elems <- c("esdata_hs2fcl_mapped_links",
             "tldata_hs2fcl_mapped_links")

  extract_rprt_elem(collection_path, prefix, elems) %>%
    bind_rows() %>%
    ungroup() %>%
    select(reporter, flow, year) %>%
    arrange(year, reporter, flow) %>%
    select(year, reporter, flow)


}
