#' Converts dataset with tradeflows from HS to FCL
#'
#' It is based on notes from FAO ESS MDB files.
#'
#' @param tradedata Trade data.
#' @param hsfclmap HS-FCL map table.
#' @param parallel Logical flag indicating whether to use
#'   parallel processing.
#'
#' @import dplyr
#' @export

convertHS2FCL <- function(tradedata, hsfclmap, parallel) {

  # Unique combinations from tradedata
  uniqtrade <- tradedata %>%
    select_(~reporter, ~flow, ~hs) %>%
    distinct()

  fcltrade <- hsInRange(uniqtrade$hs,
                        hsfclmap,
                        parallel = parallel)

  if(any(is.na(fcltrade$fcl)))
    message(paste0("Proportion of HS-codes not converted in FCL: ",
                   scales::percent(sum(is.na(fcltrade$fcl))/nrow(fcltrade))))

  tradedata <- tradedata %>%
    left_join(fcltrade,
              by = c("reporter" = "areacode",
                     "flow" = "flowname",
                     "hs" = "hs"))

  if(any(is.na(tradedata$fcl)))
    message(paste0("Proportion of tradeflows with nonmapped HS-codes: ",
                   scales::percent(sum(is.na(tradedata$fcl))/nrow(tradedata)),
                   "\nShare of value of tradeflows with nonmapped HS-codes in total value: ",
                   scales::percent(sum(tradedata$value[is.na(tradedata$fcl)], na.rm = T) /
                                     sum(tradedata$value, na.rm = T))))

  tradedata

}
