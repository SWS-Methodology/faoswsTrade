#' Set flags in trade data.
#'
#' @param type The type of flag to set: "status" or "method".
#' @param flag The value of the flag to set (e.g., "e" for estimated data).
#' @param variable The variable that gets the flag: "value", "weight",
#'   "quantity".
#' @return Populates the flag variable (generating it if not present).
#' @import dplyr
#' @export

setFlag <- function(data = NA, condition = NA, type = NA, flag = NA, variable = NA) {

  if (missing(data)) stop("'tradedata' is required.")

  if (missing(condition)) stop("'condition' is required.")

  # http://adv-r.had.co.nz/Computing-on-the-language.html
  condition_call <- substitute(condition)
  env <- list2env(data, parent = parent.frame())
  condition <- eval(condition_call, env)

  if (!is.logical(condition)) {
    stop("A logical 'condition' is required.")
  }

  if (missing(type) | !(type %in% c('status', 'method'))) {
    stop("'type' should be one of c('status', 'method').")
  }

  if (missing(flag)) {
    stop("Please, set 'flag' to one flag value.")
  } else if (type == 'status' & !(flag %in% c('E', 'I', 'X'))) {
    stop(paste("'flag' should be one of 'E', 'I', 'X'"))
  } else if (type == 'method' & !(flag %in% c('c', 'i', 'e', 's', 'h'))) {
    stop(paste("'flag' should be one of 'c', 'i', 'e', 's', 'h'"))
  }

  if (missing(variable) | !(variable %in% c('value', 'weight', 'quantity', 'all'))) {
    stop("Please, set 'variable' to 'value', 'weight', 'quantity', or 'all'")
  }

  if (type == 'status') flag <- paste0('flag_status_', flag)

  if (type == 'method') flag <- paste0('flag_method_', flag)

  if (variable == 'all') {
    res <- '1-1-1'
  } else {
    res <- paste(1*(variable=='value'), 1*(variable=='weight'), 1*(variable=='quantity'), sep='-')
  }

  res <- factor(res, levels = c("0-0-0", "0-0-1", "0-1-0", "0-1-1", "1-0-0", "1-0-1", "1-1-0", "1-1-1"))

  # Using if_else instead of ifelse as the latter changes data type
  if (flag %in% colnames(data)) {
    data[[flag]] <- dplyr::if_else(condition, res, data[[flag]])
  } else {
    data[[flag]] <- dplyr::if_else(condition, res, as.factor('0-0-0'))
  }

  return(data)
}
