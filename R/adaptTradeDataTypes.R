#' Standardise TL or ES variable names.
#'
#' Use the same variable types in both datasets: integer for year, reporter,
#'   partner, flow; numeric for value, weight and qty; character for hs and
#'   created hs6.
#'
#' @param tradedata TL or ES trade data.
#' @return TL or ES data with common data types.
#' @export

adaptTradeDataTypes <- function(tradedata) {

  if (missing(tradedata)) stop('"tradedata" should be set.')

  tradedataname <- deparse(substitute(tradedata))

  stopifnot(all(c("year", "reporter", "partner", "flow", "value",
                    "weight", "qty", "hs") %in% colnames(tradedata)))

  tradedata[, flow := as.integer(flow)]

  tradedata[, c("value", "weight", "qty") := lapply(.SD, as.numeric),
            .SDcols = c("value", "weight", "qty")]

  if (tradedataname == "tldata") {
    tradedata[, c("year", "qunit") := lapply(.SD, as.integer),
              .SDcols = c("year", "qunit")]
  } else {
    tradedata[, year := as.integer(substr(year, 1, 4))]
  }
}

