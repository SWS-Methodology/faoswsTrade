#' Standardise TL or ES variable names.
#'
#' Use the same variable names in both datasets.
#'
#' TL and ES data use different variable names for reporters, partners,
#' commodities, values, quantity, and year. This function give these
#' variables the same name in both datasets.
#'
#' @param tradedata TL or ES trade data.
#' @param origin String: "TL" or "ES", for Tariff Line or Eurostat data
#'   respectively.
#' @return TL or ES data with common names (TL will also have "qunit").
#' @import dplyr
#' @export

adaptTradeDataNames <- function(tradedata = NA, origin = NA) {

  if (missing(tradedata)) stop('"tradedata" should be set.')

  if (missing(origin) | (origin!="TL" & origin!="ES")) {
    stop('"origin" needs to be "TL" or "ES"')
  }

  if (origin == "TL") {
    tradedata <- tradedata %>%
      mutate(qunit = as.integer(qunit)) %>%
      rename(reporter = rep,
             partner  = prt,
             hs       = comm,
             year     = tyear,
             value    = tvalue)
  } else { 
    tradedata <- tradedata %>%
      mutate(period = str_sub(period,1,4)) %>%
      rename(reporter = declarant,
             partner  = partner,
             hs       = product_nc,
             year     = period,
             value    = value_1k_euro,
             weight   = qty_ton,
             qty      = sup_quantity)
  }

  tradedata %>%
    mutate(
           reporter = as.integer(reporter),
           partner  = as.integer(partner),
           flow     = as.integer(flow),
           year     = as.character(year),
           value    = as.numeric(value),
           weight   = as.numeric(weight),
           qty      = as.numeric(qty),
           hs6      = stringr::str_sub(hs,1,6)
           )
}
