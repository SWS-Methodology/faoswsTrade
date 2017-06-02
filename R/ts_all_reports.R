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
#'

ts_all_reports <- function(collection_path = NULL,
                           prefix = NULL,
                           ts_reports_path = NULL) {

  stopifnot(!is.null(collection_path))
  stopifnot(!is.null(prefix))
  stopifnot(dir.exists(collection_path))

  if (!is.null(ts_reports_path))
    stopifnot(dir.exists(ts_reports_path)) else
      ts_reports_path <- collection_path


invisible(NULL)
}
