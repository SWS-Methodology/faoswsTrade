#' Change position of flags.
#'
#' @param variable Varaible name.
#' @param swap How to swap flags (e.g., '\\2\\1\\3').
#' @param condition Logical condition that indicates which
#'   flags will be swapped (when TRUE).
#'
#' @export

swapFlags <- function(variable = stop("'variable' is required."),
                      swap = stop("'swap' is required."),
                      condition = NULL) {

  # example of swap: swap = '\\2\\1\\3'

  if (all(is.na(variable))) {
    warning("All NAs: no need to swap.", call. = TRUE)
    return(variable)
  }

  res <- sub('1(.)(.)(.)', paste0(1, swap), variable) %>%
           as.integer()

  if (is.null(condition)) {
    return(res)
  } else {
    return(ifelse(condition, res, variable))
  }
}
