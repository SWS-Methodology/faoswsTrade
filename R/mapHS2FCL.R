#' Function to map HS codes to FCL codes
#'
#' @import dplyr
#' @export

mapHS2FCL <- function(tradedata, maptable, parallel = NULL) {

  stopifnot(all(c("reporter", "flow", "hs") %in% colnames(tradedata)))

  # Align HS codes from data and table ####

  hslength <- maxHSLength(uniqhs, maptable)

  # Find mappings ####

  # Choose one from multiple matches ####

  # Join original trade dataset with mapping ####

}
