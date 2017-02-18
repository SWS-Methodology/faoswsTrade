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

  hslength <- maxHSLength(uniqhs, maptable)

  tradedata <- tradedata %>%
    left_join(hslength, by = c("reporter", "area")) %>%
    mutate_(hsextchar = ~stringr::str_pad(hs,
            width = maxhslength,
            side = "right",
            pad = "0"),
            hsext = ~as.numeric(hsextchar))

  # extend mapping

  # maptable <- maptable %>%
  #   left_join(hslength, by = c("area" = "reporter", ))
  # Find mappings ####

  # Choose one from multiple matches ####

  # Join original trade dataset with mapping ####

}
