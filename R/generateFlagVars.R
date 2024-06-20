#' Generate flag variables in trade data.
#'
#' @param data Trade data.
#' @param status The status flag that will be set.
#' @param method The method flag that will be set.
#'
#' @export

generateFlagVars <- function(data = stop("'data' needs to be set."),
                             status = c('X', 'I', 'E', 'A'),
                             method = c('c', 'i', 'e', 's', 'h')) {

  var_names <- c(paste0('flag_status_', sort(status)),
                 paste0('flag_method_', sort(method)))

  data[var_names] <- 1000L

  return(data)
}
