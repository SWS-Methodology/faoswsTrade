#' Function to map HS codes to FCL codes
#'
#' @import dplyr
#' @export

mapHS2FCL <- function(tradedata, maptable, parallel = NULL) {

  stopifnot(all(c("reporter", "flow", "hs") %in% colnames(tradedata)))

  # Extract unique input combinations ####

  uniqhs <- tradedata %>%
    select_(~reporter, ~flow, ~hs) %>%
    distinct

  # Align HS codes from data and table ####

  # Find mappings ####

  # Choose one from multiple matches ####

  # Join original trade dataset with mapping ####

}
