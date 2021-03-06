#' Remove non numeric reporter/partner/hs.
#'
#' @param tradedata TL or ES trade data.
#' @return TL or ES data without nonnumeric reporter/partner/hs.
#' @export

removeNonNumeric <- function(tradedata) {

  if (missing(tradedata)) stop('"tradedata" should be set.')

  tradedataname <- deparse(substitute(tradedata))

  stopifnot(all(c("reporter", "partner", "hs") %in% colnames(tradedata)))

  tradedata[, c("reporter", "partner") := lapply(.SD, as.character),
            .SDcols = c("reporter", "partner")]

  tradedata[, reporter_non_numeric := !grepl("^[[:digit:]]+$", reporter)]
  tradedata[, partner_non_numeric := !grepl("^[[:digit:]]+$", partner)]
  tradedata[, hs_non_numeric := !grepl("^[[:digit:]]+$", hs)]

  tradedata[, novalue := is.na(value)]

  tradedata[, noqty := is.na(weight) & is.na(qty)]

  # When building preprocess time series we need to bind TL and ES reports.
  # In his local copies of raw trade data sets Alex has differences in type
  # of flows, so we convert flow to integer before calling reports.
  #
  # It is a temporary workaround and can be removed if all sources have
  # flow as a character variable.
  tradedata$flow <- as.integer(tradedata$flow)

  # XXX: bring back
  #rprt(tradedata, "rawtradedata", tradedataname)

  tradedata[
    !(reporter_non_numeric | partner_non_numeric | hs_non_numeric)
  ][,
    paste0(c("reporter", "partner", "hs"), "_non_numeric") := NULL
  ]
}
