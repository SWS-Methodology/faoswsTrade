#' Set flags in trade data.
#'
#' The generated flag will be a numeric variable like 1XYZ where
#' X, Y and Z are dummies that are equal to 1 if the flag applies to
#' the \code{value} or \code{weight} or \code{quantity} variables,
#' respectively, and 0 otherwise.
#'
#' @param data Trade data.
#' @param condition A logical condition where the flags should be changed.
#' @param type The type of flag to set: "status" or "method".
#' @param flag The value of the flag to set (e.g., "e" for estimated data).
#' @param variable The variable that gets the flag: "value", "weight",
#'   "quantity".
#' @return Populates the flag variable.
#' @export

setFlag3 <- function(data = NA, condition = NA, type = NA, flag = NA, variable = NA) {

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
  } else if (type == 'status' & !(flag %in% c('T', 'E', 'I', 'X', 'Y'))) {
    stop(paste("'flag' should be one of 'T', 'E', 'I', 'X', 'Y'"))
  } else if (type == 'method' & !(flag %in% c('c', 'i', 'e', 's', 'h'))) {
    stop(paste("'flag' should be one of 'c', 'i', 'e', 's', 'h'"))
  }

  if (missing(variable) | !(variable %in% c('value', 'weight', 'quantity', 'all'))) {
    stop("Please, set 'variable' to 'value', 'weight', 'quantity', or 'all'")
  }

  if (type == 'status') flag <- paste0('flag_status_', flag)

  if (type == 'method') flag <- paste0('flag_method_', flag)

  if (variable == 'all') {
    res <- 1111L
  } else {
    res <- as.integer(
            1000+
             100*(variable == 'value')+
              10*(variable == 'weight')+
               1*(variable == 'quantity')
            )
  }

  if (flag %in% colnames(data) & all(!is.na(data[[flag]]))) {
    res <- sumFlags(new = res, old = data[[flag]])
    alt <- data[[flag]]
  } else {
    alt <- 1000L
  }

  data[[flag]] <- ifelse(condition %in% TRUE, res, alt)

  return(data)
}
