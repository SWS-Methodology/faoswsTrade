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
    tradedata %>%
      transmute_(reporter = ~as.integer(rep),
                 partner = ~as.integer(prt),
                 hs = ~comm,
                 flow = ~as.integer(flow),
                 year = ~as.character(tyear),
                 value = ~tvalue,
                 weight = ~weight,
                 qty = ~qty,
                 qunit = ~as.integer(qunit)) %>%
      mutate_(hs6 = ~stringr::str_sub(hs,1,6))
    
  } else { 
    tradedata %>%
      transmute_(reporter = ~as.numeric(declarant),
                 partner = ~as.numeric(partner),
                 hs = ~product_nc,
                 flow = ~as.integer(flow),
                 year = ~as.character(str_sub(period,1,4)),
                 value = ~as.numeric(value_1k_euro),
                 weight = ~as.numeric(qty_ton),
                 qty = ~as.numeric(sup_quantity)) %>%
      mutate_(hs6 = ~stringr::str_sub(hs,1,6))
  }
}
