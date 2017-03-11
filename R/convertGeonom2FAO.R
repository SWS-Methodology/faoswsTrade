#' Convertion of EuroStat Geonomenclature to FAO area codes
#'
#' @param geonom Vector with Geonom area codes
#' @import dplyr
#' @import futile.logger
#' @export

convertGeonom2FAO <- function(geonom) {

  data("geonom2fao", package = "faoswsTrade", envir = environment())

  geonom <- data_frame(geonom = geonom)

  geonom <- geonom %>%
    left_join(geonom2fao %>%
                select_(geonom = ~code,
                        faorep = ~active,
                        ~name),
              by = "geonom")

  nofaorep <- sort(unique(geonom$geonom[is.na(geonom$faorep)]))

  if(length(nofaorep) > 0)
    flog.info("Eurostat country codes were not converted to FAO area codes: %s",
              paste(nofaorep, collapse = ", "))

  flog.info("Eurostat country codes were converted to 252 FAO area code (NES):",
            geonom %>%
              filter_(~faorep == 252) %>%
              distinct %>%
              arrange_(~name) %>%
              as.data.frame,
            capture = TRUE)

  geonom$faorep

}

