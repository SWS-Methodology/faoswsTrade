#' Remove non numeric reporter/partner/hs.
#'
#' @param tradedata TL or ES trade data.
#' @return TL or ES data without nonnumeric reporter/partner/hs.
#' @import dplyr
#' @export

removeNonNumeric <- function(tradedata) {

  if (missing(tradedata)) stop('"tradedata" should be set.')

  tradedataname <- tolower(lazyeval::expr_text(tradedata))

  stopifnot(all(c("reporter", "partner", "hs") %in% colnames(tradedata)))

  tradedata <- tradedata %>%
    mutate_at(vars(reporter, partner), as.character) %>%
    mutate_at(vars(reporter, partner, hs),
            funs(non_numeric = !grepl("^[[:digit:]]+$", .))) %>%
    mutate_(novalue = ~is.na(value),
            noqty   = ~is.na(weight) & is.na(qty))

  # When building preprocess time series we need to bind TL and ES reports.
  # In his local copies of raw trade data sets Alex has differences in type
  # of flows, so we convert flow to integer before calling reports.
  #
  # It is a temporary workaround and can be removed if all sources have
  # flow as a character variable.
  tradedata$flow <- as.integer(tradedata$flow)

  rprt(tradedata, "rawtradedata", tradedataname)

  tradedata %>%
    filter_(~!(reporter_non_numeric | partner_non_numeric | hs_non_numeric)) %>%
    select_(~-ends_with("_non_numeric"))
}
