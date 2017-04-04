#' Function to filter trade data by HS6 codes of FAO interest
#'
#' Fiter out HS codes which don't participate in futher processing
#' Such solution drops all HS codes shorter than 6 digits.
#'
#' @import dplyr
#' @import futile.logger
#' @import stringr
#' @export

filterHS6FAOinterest <- function(tradedata) {

  stopifnot("hs" %in% colnames(tradedata))

  hs6faointerest <- as.integer(getAgriHSCodes())

  orignrow <- nrow(tradedata)

  flog.info("Filtering by HS6 agri codes.")
  flog.info("Records before filtering: %s", orignrow)

  tradedata <- tradedata %>%
    filter_(~hs6 %in% hs6faointerest)

  flog.info("Records after filtering: %s",
            nrow(tradedata))

  flog.info("Share of records kept after filtering: %s",
            scales::percent(nrow(tradedata)/orignrow))

  tradedata

}
