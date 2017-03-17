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

  data <- data %>%
    # XXX If already a tible, remove
    tbl_df() %>%
    mutate(
           flow      = ifelse(stringr::str_sub(measuredElementTrade, 1, 2) == '56', 1L, 2L),
           type      = ifelse(measuredElementTrade %in% c('5622', '5922'), 'value', 'qty'),
           flagTrade = paste(flagObservationStatus, flagMethod, sep = '-')
           ) %>%
    select(-measuredElementTrade, -flagObservationStatus, -flagMethod)

  data_flags <- data %>%
    select(-Value) %>%
    tidyr::spread(type, flagTrade) %>%
    rename(flag_qty = qty, flag_value = value)

  data_value <- data %>%
    select(-flagTrade) %>%
    tidyr::spread(type, Value)

  res <- left_join(data_value, data_flags)

  return(res)
}


