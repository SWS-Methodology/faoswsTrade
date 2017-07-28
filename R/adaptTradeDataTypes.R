#' Standardise TL or ES variable names.
#'
#' Use the same variable types in both datasets: integer for year, reporter,
#'   partner, flow; numeric for value, weight and qty; character for hs and
#'   created hs6.
#'
#' @param tradedata TL or ES trade data.
#' @param origin String: "TL" or "ES", for Tariff Line or Eurostat data
#'   respectively.
#' @return TL or ES data with common data types.
#' @import dplyr
#' @export

adaptTradeDataTypes <- function(tradedata, origin) {

  if (missing(tradedata)) stop('"tradedata" should be set.')

  origin <- toupper(origin)
  if (missing(origin) | (origin!="TL" & origin!="ES")) {
    stop('"origin" needs to be "TL" or "ES"')
  }

  tradedataname <- ifelse(origin == "TL", "tldata", "esdata")

  stopifnot(all(c("year", "reporter", "partner", "flow", "value", "weight",
                   "qty", "hs") %in% colnames(tradedata)))

  tradedata <- tradedata %>%
    mutate_at(vars(reporter, partner),
              as.character)

  tradedata <- tradedata %>%
    mutate_at(vars(reporter, partner, hs),
            funs(non_numeric = !grepl("^[[:digit:]]+$", .))) %>%
    mutate_(novalue = ~is.na(value),
            noqty   = ~is.na(weight) & is.na(qty))

  # When building preprocess time series we need to bind TL and ES reports. In
  # his local copies of raw trade data sets Alex has differences in type of
  # flows, so we convert flow to integer before calling preprocess reports.
  #
  # It is a temporary workaround and can be removed if all sources have flow as
  # a character variable.
  tradedata$flow <- as.integer(tradedata$flow)

  rprt(tradedata, "rawtradedata", tradedataname)

  tradedata <- tradedata %>%
    filter_(~!(reporter_non_numeric | partner_non_numeric | hs_non_numeric)) %>%
    select_(~-ends_with("_non_numeric"))

  tradedata <- tradedata %>%
    mutate_at(vars(reporter, partner, flow),
              as.integer) %>%
    mutate_at(vars(value, weight, qty),
              as.numeric) %>%
    mutate_(hs6 = ~as.integer(stringr::str_sub(hs, 1, 6)))

  if (origin == "TL") {
    tradedata %>%
      mutate_at(vars(year, qunit), as.integer)
  } else {
    tradedata %>%
      mutate_(year = ~as.integer(stringr::str_sub(year, 1, 4)))
  }
}
