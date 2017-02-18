#' Prepares for numerical comparison vector of hs codes from
#' trade dataset with hs codes range from mapping table.
#'
#' @param tradedata A data frame with columns `reporter`, `flow` and `hs`.
#' @param mapdataset Data frame with mapping table containing
#'   at least columns area, flow, fromcode, tocode
#'
#' @return A data frame with columns `reporter`, `flow` and `maxhslength`.
#'
#' @details Alignes length of hs codes in three places: vector from trade
#'   dataset, fromcode and tocode columns of mapping dataset.
#'   Maximum length is determined and all shorter codes are extended
#'   on right-hand side: trade data hs code and mapping
#'   fromcode are extended by 0, mapping tocode is extended by 9.
#'
#' @import dplyr
#' @export

maxHSLength <- function(tradedata, mapdataset, parallel = FALSE) {

  # Vectorizing over reporter and flow in tradedata

  if(length(unique(uniqhs$reporter)) > 1L |
     length(unique(uniqhs$flow)) > 1L) return(
       plyr::ddply(.data = uniqhs,
                   .variables = c("area", "flow"),
                   .parallel = parallel,
                   .fun = maxHSLength,
                   mapdataset
       )
     )


  # Filter mapping table by current reporter and flow
  mapdataset <- mapdataset %>%
    filter_(~reporter == uniqhs$reporter[1],
            ~flow == uniqhs$flow[1])

  maxhslength   <- max(stringr::str_length(uniqhs$hs))
  maxfromlength <- max(stringr::str_length(mapdataset$fromcode))
  maxtolength   <- max(stringr::str_length(mapdataset$tocode))
  uniqhs$maxhslength  <- max(maxhslength, maxtolength, maxfromlength)

  uniqhs %>%
    select_(~-hs) %>%
    distinct

  # tradedata$hs <- as.numeric(stringr::str_pad(
  #   tradedata$hs,
  #   width = maxlength,
  #   side  = "right",
  #   pad   = "0"
  # ))
  #
  # mapdataset$fromcode <- as.numeric(stringr::str_pad(
  #   mapdataset$fromcode,
  #   width = maxlength,
  #   side  = "right",
  #   pad   = "0"
  # ))
  #
  # mapdataset$tocode <- as.numeric(stringr::str_pad(
  #   mapdataset$tocode,
  #   width = maxlength,
  #   side  = "right",
  #   pad   = "9"
  # ))

}
