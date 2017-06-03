#'General function to produce all time-series reports
#'@param collection_path String with path where single-year report directories
#'  are stored.
#'@param prefix String with report directory name prefix without trailing
#'  underscore. By default NULL.
#'@param ts_reports_path String with path where time series reports should be
#'  saved. If NULL (default) the directory with TS reports will be saved where
#'  single-year reports are located (collection_path).
#'@return NULL inivisibly.
#'@export
#'@import dplyr
#'@examples ts_all_reports("/mnt/storage/sws_share/sas", "complete_tf_cpc")
#'

ts_all_reports <- function(collection_path = NULL,
                           prefix = NULL,
                           ts_reports_path = NULL) {

  stopifnot(!is.null(collection_path))
  stopifnot(!is.null(prefix))
  stopifnot(dir.exists(collection_path))

  if (!is.null(ts_reports_path)) {
    if (!dir.exists(ts_reports_path))
      dir.create(ts_reports_path,
                 recursive = TRUE,
                 showWarnings = FALSE)
      } else ts_reports_path <- collection_path

  # ts_hsfcl_nolinks(collection_path, prefix = prefix) %>%
  #   ts_write_rprt(ts_reports_path, "ts_hsfcl_nolinks")
#
#   ts_hsfcl_nolinks_statistic(collection_path, prefix = prefix) %>%
#     ts_write_rprt(ts_reports_path, "ts_hsfcl_nolinks_statistics")

  ts_preproc(collection_path, prefix = prefix) %>%
    ts_write_rprt(ts_reports_path, "ts_preproc_long_format")

  ts_reporters(collection_path, prefix = prefix) %>%
    ts_write_rprt(ts_reports_path, "ts_reporters")

  ts_non_reporters(collection_path, prefix = prefix) %>%
    ts_write_rprt(ts_reports_path, "ts_non_reporters")

invisible(NULL)
}
