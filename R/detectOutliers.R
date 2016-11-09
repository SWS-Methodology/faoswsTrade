#' Detect anomalous observations (outliers).
#'
#' An anomalous unit value will be indicated as "outlier".
#'
#' @param tradedata Trade data.
#' @param method String: method to use for outlier detection. Currently
#'   "boxplot" is allowed.
#' @param parameters List: named parameters to pass to \code{method}.
#' @return \code{tradedata} with an additional "outlier" column: TRUE
#'   indicates an outlier.
#' @import dplyr
#' @export

detectOutliers <- function(tradedata = NA, method = NA, parameters = NA) {

  if (missing(tradedata)) stop('"tradedata" is missing.')

  if (missing(method)) stop('"method" is missing.')

  if (missing(parameters)) stop('"parameters" is missing.')

  if (method == "boxplot") {
    if (!("out_coef" %in% names(parameters))) {
        stop('"parameters" list should contain a "out_coef" scalar.')
    }

    tradedata %>%
      group_by_(~year, ~reporter, ~flow, ~hs) %>%
      mutate_(outlier = ~uv %in% boxplot.stats(uv,
                                               coef = parameters$out_coef,
                                               do.conf = FALSE)$out) %>%
      ungroup()
  } else {
    stop('Only "boxplot" is currently allowed.')
  }
}
