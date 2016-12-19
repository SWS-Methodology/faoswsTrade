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

alignHSLength <- function(hs, mapdataset) {
  
  stopifnot(length(unique(mapdataset$area)) == 1)
  stopifnot(length(unique(mapdataset$flow)) == 1)
  stopifnot(all(stringr::str_detect(hs, "^\\d+$")))
  stopifnot(all(stringr::str_detect(mapdataset$fromcode, "^\\d+$")))
  stopifnot(all(stringr::str_detect(mapdataset$tocode, "^\\d+$")))
  
  maxhslength   <- max(stringr::str_length(hs))
  maxfromlength <- max(stringr::str_length(mapdataset$fromcode))
  maxtolength   <- max(stringr::str_length(mapdataset$tocode))
  maxlength     <- max(maxhslength, maxtolength, maxfromlength)
  
  hs <- as.numeric(stringr::str_pad(
    hs,
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
  
  list(hs = hs,
       mapdataset = mapdataset)
  
}