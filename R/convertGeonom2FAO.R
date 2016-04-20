#' Convertion of EuroStat Geonomenclature to FAO area codes
#'
#' @import dplyr
#' @export

convertGeonom2FAO <- function(geonom) {

  data("geonom2fao", package = "faoswsTrade", envir = environment())

  geonom <- data_frame(geonom = geonom)

  geonom <- geonom %>%
    left_join(geonom2fao %>%
                select_(geonom = ~code,
                        faorep = ~active),
              by = "geonom")

  nofaorep <- geonom$geonom[is.na(geonom$faorep)]

  if(length(nofaorep) > 0)
    warning(paste0("These ES Geonom codes were not converted to FAO area list: ",
                   paste0(nofaorep, collapse = ", ")))

  geonom$faorep

}
