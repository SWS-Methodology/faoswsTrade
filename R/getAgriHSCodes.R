#' Returns subset of HS-codes of agriculture items used by ESS devision.
#'
#' List of agricultural commodities provided by Cladia DeVita.
#'@return Character vector
#'@export

getAgriHSCodes <- function() {

  hs6faointerest <- try(faosws::ReadDatatable('hs6faointerest'))

  if (inherits(hs6faointerest, 'try-error')) {
    stop('The "hs6faointerest" table is not available.')
  }

  return(hs6faointerest$hs6_code)
}
