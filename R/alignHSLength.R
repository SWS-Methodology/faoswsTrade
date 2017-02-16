#' Prepares for numerical comparison vector of hs codes from
#' trade dataset with hs codes range from mapping table.
#'
#' @param hs Vector of original hs codes from trade data set.
#' @param mapdataset Data frame with mapping table containing
#'   at least columns area, flow, fromcode, tocode
#'
#' @return A list with two components:
#'   * hs - vector with extended hs codes
#'   * mapdataset - data frame with extended fromcode and to code
#'   columns.
#'
#' @details Alignes length of hs codes in three places: vector from trade
#'   dataset, fromcode and tocode columns of mapping dataset.
#'   Maximum length is determined and all shorter codes are extended
#'   on right-hand side: trade data hs code and mapping
#'   fromcode are extended by 0, mapping tocode is extended by 9.
#'
#' @export

alignHSLength <- function(tradedata, mapdataset, parallel = FALSE) {

  stopifnot(all(stringr::str_detect(tradedata$hs, "^\\d+$")))
  stopifnot(all(stringr::str_detect(mapdataset$fromcode, "^\\d+$")))
  stopifnot(all(stringr::str_detect(mapdataset$tocode, "^\\d+$")))

  # Vectorizing over reporter and flow in tradedata

  if(length(unique(tradedata$reporter)) > 1 |
     length(unique(tradedata$flow)) > 1)
    return(plyr::dlply(tradedata,
                       c("area", "flow"),
                       function(df) alignHSLength(
                         df, mapdataset,
                         .parallel = parallel)))


  # Filter mapping table by current reporter and flow
  mapdataset <- mapdataset %>%
    filter_(~reporter == tradedata$reporter[1],
            ~flow == tradedata$flow[1])

  maxhslength   <- max(stringr::str_length(tradedata$hs))
  maxfromlength <- max(stringr::str_length(mapdataset$fromcode))
  maxtolength   <- max(stringr::str_length(mapdataset$tocode))
  maxlength     <- max(maxhslength, maxtolength, maxfromlength)

  tradedata$hs <- as.numeric(stringr::str_pad(
    tradedata$hs,
    width = maxlength,
    side  = "right",
    pad   = "0"
  ))

  mapdataset$fromcode <- as.numeric(stringr::str_pad(
    mapdataset$fromcode,
    width = maxlength,
    side  = "right",
    pad   = "0"
  ))

  mapdataset$tocode <- as.numeric(stringr::str_pad(
    mapdataset$tocode,
    width = maxlength,
    side  = "right",
    pad   = "9"
  ))

  list(tradedata = tradedata,
       mapdataset = mapdataset)

}
