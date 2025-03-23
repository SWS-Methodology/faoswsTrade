#' Sum the flags indicator variables
#'
#' This function will sum two indicator variables of the type
#' XYZ where X, Y or Z are 0 or 1, and the result will be itself
#' a XYZ variable. Note: if \code{new} and \code{old} are used,
#' \code{flags} should not be used, and vice versa.
#'
#' @param new The new flag. Should be a scalar.
#' @param old The old flags. Can be a vector.
#' @param flags A numeric vector of flags to be summed.
#' @return An indicator variable.
#' @export

sumFlags <- function(new = NA, old = NA, flags = NA) {

  if (!missing(new) & !missing(old) & !missing(flags)) {
    stop('"new", "old", and "flags" cannot be used toghether')
  }

  if (missing(flags) & (missing(new) | missing(old))) {
    stop('One of "new" or "old" are missing.')
  }

  if (!missing(flags)) {
    if (!is.numeric(flags)) {
      stop('"flags" should be numeric.')
    }

    res <- sum(unique(flags))
  }

  if (!missing(new) & !missing(old)) {
    if (!is.numeric(new) | !is.numeric(old)) {
      stop('"new" and "old" should be numeric.')
    }

    res <- new + old
  }

  as.integer(gsub('[^0]', '1', res))
}
