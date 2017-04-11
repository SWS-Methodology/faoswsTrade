#' Moving average
#'
#' Compute the moving average of a vector.
#'
#' The resulting moving average will be of order three.
#'
#' @param x a numeric vector.
#' @param pkg string: "native" (default) or the name of a package with
#'   a function that computes moving averages. Besides "native", only
#'   "zoo" is allowed (\code{zoo::rollapply} will be used).
#'   respectively.
#' @param mode String: "centered" (default) indicates that the moving
#'   average should be centered.
#'
#' @return A vector with the moving average of the input.
#'
#' @export

movav <- function(x, pkg = 'native', mode = 'centered', n = 5, na.rm = TRUE) {

  if (pkg == 'zoo') {
    if (mode == 'centered') {
      stop('Only "mode = centered" is implemented with zoo')
    } else {
      res <- zoo::rollapply(lag(x), n, mean, fill = NA,
                            align = 'right', na.rm = na.rm)
    }
  } else if (pkg = 'native') {
    if (n != 3) stop('Only n = 3 can be used with pkg = "native"')

    if (mode == 'centered') {
      if (length(x) > 1) {
        res <- cbind(x, c(NA, x[1:(length(x)-1)]), c(x[2:length(x)], NA))
        res <- apply(res, 1, mean, na.rm = TRUE)
      } else {
        res <- x
      }
    } else {
      if (length(x) > 2) {
        res <- cbind(x, c(NA, x[1:(length(x)-1)]), c(NA, NA, x[1:(length(x)-2)]))
        res <- apply(res, 1, mean, na.rm = TRUE)
        res[1:2] <- res[3]
      } else {
        res <- x
      }
    } else {
      stop('"pkg" should be "native" or "zoo"')
    }

    res[is.nan(res)] <- NA

    res <- ifelse(!is.na(x), res, NA)
  }

  if (length(x) != length(res)) {
    stop('The lengths of input and output differ.')
  }

	return(res)
}
