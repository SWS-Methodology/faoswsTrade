#' Generate flag variables in trade data.
#'
#' @export

generateFlagVars <- function(data = stop("'data' needs to be set."),
                             status = c('E', 'I', 'X'),
                             method = c('c', 'i', 'e', 's', 'h')) {
  
  var_names <- c(paste0('flag_status_', sort(status)),
                 paste0('flag_method_', sort(method)))

  data[var_names] <- 1000L

  return(data)
}
