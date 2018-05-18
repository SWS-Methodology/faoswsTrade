#' Add "finalunits" variable to trade data.
#'
#' For each CPC code an expected CPC unit (i.e., what units does FAO expects
#' for given CPC codes) is added.
#'
#' @param tradedata Trade data.
#' @param finalunits A table containing CPC codes and expected measure units.
#' @return \code{tradedata} with \code{finalunits} added.
#' @import dplyr
#' @export

addFinalUnits <- function(tradedata = NA, finalunits = NA) {

  if (missing(tradedata)) stop('"tradedata" is missing.')

  if (missing(finalunits)) stop('"finalunits" is missing.')

  tradedata %>%
    left_join(finalunits, by = c("cpc", "fcl")) %>%
    ## NA finalunits has to be set up as "mt" (suggest by Claudia)
    mutate_(finalunit = ~ifelse(is.na(finalunit), "mt", finalunit))

}
