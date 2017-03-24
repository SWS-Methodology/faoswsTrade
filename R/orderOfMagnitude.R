#' Impute data with problems in the reported order of magnitude.
#'
#' Fix problems with probable misreported numbers by a factor of 10.
#'
#' It can happen that a number is reported as, e.g., 36 instead of 3.6
#' or 24 instead of 240. This may be to the fact that either there was
#' a human error or an error in the reported quantity (e.g., grams instead
#' of kilograms). In any case, this problem can be solved by checking
#' whether the reported number and a benchmark differ by a factor of 10.
#' Note: by default there is a +/- 10% discrepancy allowance in the reported
#' number to the benchmark (e.g., if the benchmark is 36, then 360 will be
#' transformed to 36 and 396 to 39.6, but 400 will not be converted to 40).
#' This can be changed with the \code{allow} parameter.
#'
#' @param to.check A character string that indicates the column with data
#'   to eventually correct.
#' @param benchmark A character string that indicates the column with data
#'   to be considered benchmark.
#' @param method Which method to use to round the ratio.
#' @param allow Percentage of allowed discrepancy. By default it is 10 = 10%.
#' @param condition An optional logical condition that should be valid in
#'   order for the correction to be applied.
#' @return A number, corrected if the discrepancy between \code{to.check}
#'   and \code{benchmark} is of an order of a factor of 10.
#' @export

orderOfMagnitude <- function(data = NA,
                             to.check = NA,
                             benchmark = NA,
                             method = 'round',
                             condition = NA,
                             allow = 10) {

  if (missing(data)) stop("'data' is required.")
  if (missing(to.check)) stop("'to.check' is required.")
  if (missing(benchmark)) stop("'bechmark' is required.")

  to.check_name <- to.check
  benchmark_name <- benchmark

  to.check <- data[[to.check]]
  benchmark <- data[[benchmark]]

  if (is.null(to.check)) {
    stop(paste0('There is no "', to.check_name, '" variable.'))
  }

  if (is.null(to.check)) {
    stop(paste0('There is no "', benchmark_name, '" variable.'))
  }

  if (!missing(condition)) {
    # http://adv-r.had.co.nz/Computing-on-the-language.html
    condition_call <- substitute(condition)
    env <- list2env(data, parent = parent.frame())
    condition <- eval(condition_call, env)

    if (!is.logical(condition)) {
      stop("A logical 'condition' is required.")
    }
  }

  if (method == 'round') {
    ratio <- 10^(round(log10(to.check/benchmark)))
  } else if (method == 'ceiling') {
    ratio <- 10^(ceiling(log10(to.check/benchmark)))
  } else {
    stop('"method" should be one of "round" or "ceiling".')
  }

  test <- (to.check >= benchmark * ratio * (100-allow)/100) &
          (to.check <= benchmark * ratio * (100+allow)/100)

  ifelse(test, to.check/ratio, to.check)
}
