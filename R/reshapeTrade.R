#' Reshape trade data.
#'
#' @param data data.
#'
#' @return Reshaped data.
#'
#' @import dplyr
#'
#' @export

reshapeTrade <- function(data = NA) {

  data <-
    data %>%
    tbl_df() %>%
    dplyr::mutate(
      flow      = ifelse(stringr::str_sub(measuredElementTrade, 1, 2) == '56', 1, 2),
      type      = stringr::str_sub(measuredElementTrade, 3, 4),
      type      = ifelse(type == '22', 'value', ifelse(type == '10', 'tonne', 'head')),
      flagTrade = paste(flagObservationStatus, flagMethod, sep = '-')
    ) %>%
    select(-flagObservationStatus, -flagMethod, -measuredElementTrade)

  data_num <-
    data %>%
    select(-flagTrade) %>%
    tidyr::spread(type, Value)

  data_flags <-
    data %>%
    select(-Value) %>%
    dplyr::mutate(type = paste0('flag_', type)) %>%
    tidyr::spread(type, flagTrade)

  res <-
    left_join(
      data_num,
      data_flags,  
      by = c(
        'geographicAreaM49Reporter',
        'geographicAreaM49Partner',
        'measuredItemCPC',
        'timePointYears',
        'flow'
      )
    ) %>%
    dplyr::mutate(
      livestock   = !is.na(head),
      qty         = ifelse(livestock, head, tonne),
      weight      = ifelse(livestock, tonne, NA),
      flag_qty    = ifelse(livestock, flag_head, flag_tonne),
      flag_weight = ifelse(livestock, flag_tonne, NA)
    ) %>%
    select(-head, -tonne, -flag_head, -flag_tonne, -livestock)

  return(res)
}

