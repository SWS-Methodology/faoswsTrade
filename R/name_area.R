#' A function to provide names for codes from different area classifications.
#'
#' @param areacode Vector of area list codes.
#' @param code_class A character string indicating classification of area codes:
#' "fao", "m49" or "geonom".
#'
#' @return Character vector of area names.
#'
#' @export
#' @import dplyr

name_area <- function (areacode, code_class = NULL) {

  if(is.null(code_class)) stop("code_class is not provided.")
  stopifnot(code_class %in% c("fao", "m49", "geonom"))

  load_fao <- function() {
    #data("faocountrycode", package = "faoswsTrade", envir = environment())
    fao_country_code <-
      GetCodeList(
        domain    = 'faostat_one',
        dataset   = 'FS1_SUA_UPD',
        dimension = 'geographicAreaFS'
      )[, list(area = as.numeric(code), area_name = description)]
  }

  load_m49 <- function() {
    #data("unsdpartnersblocks", package = "faoswsTrade", envir = environment())
    unsdpartnersblocks <- ReadDatatable("unsdpartnersblocks")
    data("m49", package = "faoswsTrade", envir = environment())

    bind_rows(
    m49 %>%
      select_(area = ~code, area_name = ~abbr) %>%
      distinct(),
    unsdpartnersblocks %>%
      select(area = unsdpb_formula, area_name = unsdpb_crnamee_1) %>%
      distinct() %>%
      dplyr::filter(!(area %in% m49$code))
    )
  }

  load_geonom <- function() {
    #data("geonom2fao", package = "faoswsTrade", envir = environment())
    geonom2fao <- ReadDatatable("geonom2fao")

    geonom2fao %>%
      select_(area = ~code, area_name = ~name) %>%
      distinct() %>%
      mutate_(area_name = ~stringr::str_trim(area_name))
  }

  namestable <- switch(code_class,
                       fao = load_fao(),
                       m49 = load_m49(),
                       geonom = load_geonom())

  areacode <- as.integer(areacode)

  df <- data_frame(areacode = areacode)

  df <- df %>%
    left_join(namestable, by = c("areacode" = "area"))

  df$area_name
}
