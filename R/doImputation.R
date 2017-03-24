#' Impute missing values or outliers.
#'
#' Use a median unit value to impute missing quantities or outliers.
#'
#' The median unit value is calculated in a specific-to-generic fashion:
#' 1) by HS-reporter; 2) by HS8-reporter; by HS6-reporter; 3) by HS;
#' 4) by FCL; 5) by flow.
#' The first available of these options will be used (for HS, HS8 and HS6
#' only when there are at least 10 flows).
#'
#' @param tradedata Trade data.
#' @return \code{tradedata} with imputed values.
#' @import dplyr
#' @export

doImputation <- function(tradedata = NA, uv.median = 'uvm') {

  if (missing(tradedata)) stop('"tradedata" is missing.')

  if (is.null(tradedata[[uv.median]])) {
    stop(paste0('No "', uv.median, '" variable in the data.'))
  }

  if (is.null(tradedata$value)) {
    stop(paste0('No "', value, '" variable in the data.'))
  }

  imputed_qty <- tradedata$value / tradedata[[uv.median]]

  tradedata %>%
    mutate_(qty = ~ifelse(no_quant | outlier, imputed_qty, qty),
           flagTrade = ~ifelse(no_quant | outlier, 1, 0))
}
