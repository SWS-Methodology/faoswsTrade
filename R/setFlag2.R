#' Set flags in trade data.
#'
#' @param type The type of flag to set: "status" or "method".
#' @param flag The value of the flag to set (e.g., "e" for estimated data).
#' @param variable The variable that gets the flag: "value", "quantity".
#' @return Populates the flag variable (generating it if not present).
#' @import dplyr
#' @export

setFlag2 <- function(data = NA, condition = NA, type = NA, flag = NA, variable = NA) {

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

  if (missing(variable) | !(variable %in% c('value', 'quantity', 'all'))) {
    stop("Please, set 'variable' to 'value', 'quantity', or 'all'")
  }

  # This function adds 1 to X or Y or Z in a string of the type
  # X-Y-Z where X, Y or Z are 0 or 1. If another 1 was already
  # present, it won't change
  .addFlag <- function(.newFlag, .oldFlag) {

    tmp1 <- .oldFlag %>%
      strsplit('-') %>%
      do.call(rbind, .) %>%
      apply(2, as.numeric) %>%
      t()

    tmp2 <- as.numeric(strsplit(.newFlag, '-')[[1]])

    apply(1*(t(tmp1 + tmp2) > 0), 1, paste, collapse='-', sep='')
  }

  if (type == 'status') flag <- paste0('flag_status_', flag)

  if (type == 'method') flag <- paste0('flag_method_', flag)

  if (variable == 'all') {
    res <- '1-1'
  } else {
    res <- paste(1*(variable=='value'), 1*(variable=='quantity'), sep='-')
  }

  if (flag %in% colnames(data) & all(!is.na(data[[flag]]))) {
    res <- .addFlag(res, as.character(data[[flag]]))
    alt <- data[[flag]]
  } else {
    alt <- '0-0'
  }

  data[[flag]] <- ifelse(condition, res, alt)

  return(data)
}
