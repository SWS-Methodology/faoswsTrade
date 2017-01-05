#' Set flags in trade data.
#'
#' @param type The type of flag to set: "observation" or "status".
#' @param flag The value of the flag to set (e.g., "e" for estimated data).
#' @param variable The variable that gets the flag: "value", "weight",
#'   "quantity".
#' @return Populates the flag variable (generating it if not present).
#' @import dplyr
#' @export

setFlag <- function(type = NA, flag = NA, variable = NA) {

  if (missing(type) | !(type %in% c('value', 'quantity', 'weight'))) {
    stop("'type' should be one of c('value', 'quantity', 'weight').")
  }

  if (missing(flag)) {
    stop("Please, set 'flag' to one flag value.")
  } else if (type == 'observation' & !(flag %in% c('E', 'I', 'X'))) {
    stop(paste("'flag' should be one of 'E', 'I', 'X'"))
  } else if (type == 'status' & !(flag %in% c('c', 'i', 'e', 's', 'h'))) {
    stop(paste("'flag' should be one of 'c', 'i', 'e', 's', 'h'"))
  }

  if (missing(variable) | !(variable %in% c('value', 'quantity', 'weight'))) {
    stop("Please, set 'variable' to 'value', 'quantity', or 'weight'")
  }

  if (type == 'observation') flag <- paste0('flag_status_', toupper(flag))

}
