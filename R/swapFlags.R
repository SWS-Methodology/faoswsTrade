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

  lev <- sort(unique(variable))

  new_val <- sub('([01])-([01])-([01])', swap, as.character(variable))

  new_lev <- sort(unique(new_val))

  if (!identical(lev, new_lev)) {
    lev <- new_lev
  }

  res <- factor(new_val, levels = lev)

  if (is.null(condition)) {
    return(res)
  } else {
    return(dplyr::if_else(condition, res, variable))
  }
}
