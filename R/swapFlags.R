#' Change position of flags.
#'
#' @export

swapFlags <- function(variable = stop("'variable' is required."),
                      swap = stop("'swap' is required."),
                      condition = NULL) {

  # example of swap: swap = '\\2-\\1-\\3'

  if (all(is.na(x))) {
    warning("All NAs: no need to swap.", call. = TRUE)
    return(variable)
  }

  res <- sub('([01])-([01])-([01])', swap, as.character(variable))

  if (is.null(condition)) {
    return(res)
  } else {
    return(ifelse(condition, res, variable))
  }
}
