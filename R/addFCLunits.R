#' Add FCL variable to trade data.
#'
#' For each FCL code an expected FCL unit (i.e., what units does FAO expects
#' for given FCL codes) is added.
#'
#' @param tradedata Trade data.
#' @param fclunits A table containing FCL codes and expected measure units.
#' @return \code{tradedata} with \code{fclunits} added.
#' @import dplyr
#' @export

addFCLunits <- function(tradedata = NA, fclunits = NA) {

  if (missing(tradedata)) stop('"tradedata" is missing.')

  if (missing(fclunits)) stop('"fclunits" is missing.')

  tradedata %>%
    left_join(fclunits, by = "fcl") %>%
    ## NA fclunits has to be set up as "mt" (suggest by Claudia)
    mutate_(fclunit=~ifelse(is.na(fclunit), "mt", fclunit))

}
