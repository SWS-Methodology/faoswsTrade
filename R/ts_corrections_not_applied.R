#' Function to produce the list of corrections not applied.
#'
#' @param collection_path String with path where single-year report directories
#'   are stored.
#'
#' @param prefix String with report directory name prefix without trailing
#'   underscore. By default NULL.
#'
#' @return A table with corrections that were not applied.
#'
#' @export
#' @import dplyr
#' @examples ts_corrections_not_applied("/mnt/storage/sws_share/sas", "complete_tf_cpc")
#'

ts_corrections_not_applied <- function(collection_path = NULL, prefix = NULL) {

  files_info <-
    dir(
      collection_path,
      pattern    = paste0(sub('_$', '', prefix), '_[0-9]{4}'),
      full.names = TRUE
    ) %>%
    data_frame(file = .) %>%
    dplyr::arrange(file) %>%
    dplyr::mutate(year = sub('.*_([0-9]{4})_.*', '\\1', file)) %>%
    group_by(year) %>%
    dplyr::filter(last(file) == file) %>%
    ungroup()

  files_info$file %>%
    sapply(
      function(x) dir(paste0(x, '/datadir'),
      pattern   = 'corrections_not_applied.rds', full.names = TRUE),
      USE.NAMES = FALSE
    ) %>%
    unlist() %>%
    lapply(readRDS) %>%
    bind_rows()
}
