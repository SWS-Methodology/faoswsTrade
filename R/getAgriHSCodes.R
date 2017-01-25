#' Returns subset of HS-codes of agriculture items used by ESS devision.
#'
#' List of agricultural commodities provided by Cladia DeVita.
#'@return Character vector
#'@export

getAgriHSCodes <- function() {

  # Eventually table should be moved in ad hoc SWS table

  data("hs6faointerest", package = "faoswsTrade", envir = environment())

  hs6faointerest
}
