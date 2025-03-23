#' Other version of adding of trailing digits. We should check it.
#'
#' @export
#'

trailingDigits2 <- function(client, maxlength, digit) {
  if(any(is.na(maxlength))) stop("NA values in maxlength. Stopping.")
  howmany <- maxlength - stringr::str_length(client)
  paste0(client,
         vapply(howmany,
                FUN = function(x) {
                  paste0(rep.int(digit, times = x), collapse = "")
                },
                FUN.VALUE = character(1)
         ))
}

