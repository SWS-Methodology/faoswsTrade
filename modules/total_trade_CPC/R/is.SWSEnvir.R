#' Function to check is it SWS environment
#' 
#' Detecting existence of SWS Envir in calling env
#' by looking for swsContext.datasets
#' 
#' @param varToCheck Variable to check.
#'
#' @return logical value. TRUE if there
#' @export

is.SWSEnvir <- function(varToCheck = "swsContext.datasets") {
  exists(varToCheck)
}
