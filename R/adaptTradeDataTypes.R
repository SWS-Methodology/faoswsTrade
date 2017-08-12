#' Standardise TL or ES variable names.
#'
#' Use the same variable types in both datasets: integer for year, reporter,
#'   partner, flow; numeric for value, weight and qty; character for hs and
#'   created hs6.
#'
#' @param tradedata TL or ES trade data.
#' @return TL or ES data with common data types.
#' @import dplyr
#' @export

adaptTradeDataTypes <- function(tradedata) {

  if (missing(tradedata)) stop('"tradedata" should be set.')

  tradedataname <- tolower(lazyeval::expr_text(tradedata))

  stopifnot(all(c("year", "reporter", "partner", "flow", "value",
                    "weight", "qty", "hs") %in% colnames(tradedata)))

  tradedata <- tradedata %>%
    mutate_at(vars(reporter, partner, flow), as.integer) %>%
    mutate_at(vars(value, weight, qty), as.numeric) %>%
    mutate_(hs6 = ~as.integer(stringr::str_sub(hs, 1, 6)))

  if (tradedataname == "tldata") {
    tradedata %>%
      mutate_at(vars(year, qunit), as.integer)
  } else {
    tradedata %>%
      mutate_(year = ~as.integer(stringr::str_sub(year, 1, 4)))
  }
}
