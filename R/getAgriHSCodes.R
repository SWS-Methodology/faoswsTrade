#' Returns subset of HS-codes of agriculture items used by ESS devision.
#'
#' List of agricultural commodities provided by Cladia DeVita.
#'@return Character vector
#'@export

getAgriHSCodes <- function() {

  hs6faointerest <- faosws::ReadDatatable('hs6faointerest')

  return(hs6faointerest$hs6_code)
}
