#' Change position of flags.
#'
#' @export

swapFlags <- function(variable = stop("'variable' is required."),
                      swap = stop("'swap' is required."),
                      condition = NULL) {

  # example of swap: swap = '\\2-\\1-\\3'

  if (!is.factor(variable)) {
    warning("Not a factor. Returning the variable unmodified", call. = TRUE)
    return(variable)
  }

  new_val <- sub('([01])-([01])-([01])', swap, as.character(variable))

  lev <- sort(unique(new_val))

  res <- factor(new_val, levels = lev)

  if (is.null(condition)) {
    return(res)
  } else {
    return(dplyr::if_else(condition, res, variable))
  }
}
