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

adaptTradeDataNames <- function(tradedata, origin) {

  if (missing(tradedata)) stop('"tradedata" should be set.')

  origin <- toupper(origin)
  if (missing(origin) | (origin!="TL" & origin!="ES")) {
    stop('"origin" needs to be "TL" or "ES"')
  }

  if (origin == "TL")
    old_common_names <- c(
      "tyear", "rep", "prt",
      "flow", "comm", "tvalue",
      "weight", "qty")

  if (origin == "ES")
    old_common_names <- c(
      "period", "declarant", "partner",
      "flow", "product_nc", "value_1k_euro",
      "qty_ton", "sup_quantity")

  new_common_names <- c("year", "reporter", "partner",
                        "flow", "hs", "value",
                        "weight", "qty")

  stopifnot(length(old_common_names) ==
                      length(new_common_names))

  tradedata %>%
    rename_(.dots = setNames(old_common_names, new_common_names))

}
