#' A function to provide names for FAO area list codes.
#'
#' @param areacode Vector of FAO area list codes.
#'
#' @return Character vector of area names.
#'
#' @export
#' @import dplyr

area_name_fao <- function (areacode) {

  #data("faocountrycode", package = "faoswsTrade", envir = environment())
  fao_country_code <-
    GetCodeList(
      domain    = 'faostat_one',
      dataset   = 'FS1_SUA_UPD',
      dimension = 'geographicAreaFS'
    )[, list(reporter = as.numeric(code), country_name = description)]

  areacode <- as.integer(areacode)

  df <- data_frame(areacode = areacode)

  df <- df %>%
    left_join(fao_country_code, by = c("areacode" = "reporter"))

  df$country_name
}
