#' Function to filter trade data by HS6 codes of FAO interest
#'
#' Fiter out HS codes which don't participate in futher processing
#' Such solution drops all HS codes shorter than 6 digits.
#'
#' @import dplyr
#' @import stringr
#' @export

filterHS6FAOinterest <- function(tradedata) {

  stopifnot("hs" %in% colnames(tradedata))

  hs6faointerest <- getAgriHSCodes()

  tradedata <- tradedata %>%
    mutate_(hs6 = ~str_extract(hs, "^\\d{6}")) %>%
    filter_(~!is.na(hs6)) %>%
    filter_(~hs6 %in% hs6faointerest) %>%
    select_(~-hs6)

  tradedata

}
