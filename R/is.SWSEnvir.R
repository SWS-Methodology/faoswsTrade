#' Function to check is it SWS environment
#' 
#' Detecting existence of SWS Envir in calling env
#' by looking for swsContext.datasets
#' 
#' @return logical value. TRUE if there 

is.SWSEnvir <- function(varToCheck = "swsContext.datasets") {
  exists(varToCheck)
}