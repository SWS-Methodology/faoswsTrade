#' General function to produce all time-series reports
#' @param collection_path String with path where single-year report directories
#'   are stored.
#' @param prefix String with report directory name prefix without trailing
#'   underscore. By default NULL.
#' @param ts_reports_path String with path where time series reports should be
#'   saved. If NULL (default) the directory with TS reports will be saved where
#'   single-year reports are located (collection_path).
#' @param complete Logical If TRUE (default) all reports (raw data, mapping,
#'   etc.) will be generated, if FALSE only raw data reports will be produced.
#' @return NULL inivisibly.
#' @export
#' @import dplyr
#' @examples ts_all_reports("/mnt/storage/sws_share/sas", "complete_tf_cpc")

ts_all_reports <- function(collection_path = NULL,
                           prefix = NULL,
                           ts_reports_path = NULL,
                           complete = TRUE) {

  stopifnot(!is.null(collection_path))
  stopifnot(!is.null(prefix))
  stopifnot(dir.exists(collection_path))
  stopifnot(is.logical(complete))

  if (!is.null(ts_reports_path)) {
    if (!dir.exists(ts_reports_path)) {
      dir.create(ts_reports_path, recursive = TRUE, showWarnings = FALSE)
    }
  } else {
    ts_reports_path <- collection_path
  }

  ts_reporters(collection_path, prefix = prefix) %>%
    ts_write_rprt(ts_reports_path, "Table_1_ts_reporters")

  ts_non_reporters(collection_path, prefix = prefix) %>%
    ts_write_rprt(ts_reports_path, "Table_2_ts_non_reporters")

  ts_count_records(collection_path, prefix = prefix) %>%
    ts_write_rprt(ts_reports_path, "Table_3_ts_preproc_long_format")

  ts_content_check(collection_path, prefix = prefix) %>%
    ts_write_rprt(ts_reports_path, "Table_4_ts_check_content")

  ts_flows_check(collection_path, prefix = prefix) %>%
    ts_write_rprt(ts_reports_path, "Table_6_ts_flows_check")

  ts_preproc(collection_path, prefix = prefix) %>%
    ts_write_rprt(ts_reports_path, "ts_preproc_long_format")

  # To be used only if the module ran completely (i.e., until the end)
  if (complete) {
    ts_hsfcl_nolinks(collection_path, prefix = prefix) %>%
      ts_write_rprt(ts_reports_path, "ts_hsfcl_nolinks")

    ts_hsfcl_nolinks_statistic(collection_path, prefix = prefix) %>%
      ts_write_rprt(ts_reports_path, "ts_hsfcl_nolinks_statistics")
  }

  invisible(NULL)
}
