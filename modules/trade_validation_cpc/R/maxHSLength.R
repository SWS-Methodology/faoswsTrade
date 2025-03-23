#' Prepares for numerical comparison vector of hs codes from
#' trade dataset with hs codes range from mapping table.
#'
#' @param uniqhs Data frame with columns `reporter`, `flow` and `hs` containing
#' unique combinations from a trade data set.
#' @param mapdataset Data frame with mapping table containing
#'   at least columns area, flow, fromcode, tocode
#'
#' @return A data frame with columns `reporter`, `flow` and `maxhslength`.
#'
#' @import dplyr
#' @export

maxHSLength <- function(uniqhs, mapdataset) {
  tab_data <- uniqhs %>%
    dplyr::mutate(hslength_data = nchar(hs)) %>%
    group_by(reporter, flow) %>%
    dplyr::summarise(maxhslength_data = max(hslength_data)) %>%
    ungroup()

  tab_map <- mapdataset %>%
    dplyr::mutate(hslength_from = nchar(fromcode), hslength_to = nchar(tocode)) %>%
    group_by(area, flow) %>%
    dplyr::summarise(maxhslength_from = max(hslength_from), maxhslength_to = max(hslength_to)) %>%
    ungroup()

  tab_join <- left_join(tab_data, tab_map, by = c('reporter' = 'area', 'flow')) %>%
    rowwise() %>%
    dplyr::mutate(maxhslength = max(maxhslength_data, maxhslength_from, maxhslength_to)) %>%
    ungroup() %>%
    dplyr::select(reporter, flow, maxhslength)

  apply(tab_join, 1, function(x) if (is.na(x['maxhslength']))
    message(paste0("For reporter ", x['reporter'], " flow ",
      x['flow'], ", no records in the mapping table")))

  return(tab_join)
}
