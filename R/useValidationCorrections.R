#' Use the corrections deriving from validation.
#'
#' @param data complete_trade_flow_cpc dataframe.
#' @param corrections Corrections table.
#' @return A list with a dataframe of corrections to drop and a
#'   subset of complete_trade_flow_cpc with corrections applied.
#' @import dplyr
#' @import data.table
#' @export

useValidationCorrections <- function(data, corrections) {

  corrections <- as.data.table(corrections)

  # NOTE: no aggregation function is indicated in the dcast as NO
  # duplicate corrections (by reporter, partner, year, item, flow,
  # data_type, and eventually by correction_level) should exist.
  # If a duplicate exists dcast will correctly say "Aggregate
  # function missing, defaulting to 'length'". The table with
  # corrections should be checked for duplicates (should be removed).
  # (Indicating and aggregation function does not make sense: if
  # there is just a combination of corrections it will give it. This
  # is the required behavious. If there are duplicates no aggregation
  # should be used.)

  corrections <- dcast(
    corrections,
    reporter + partner + item + flow ~ data_type,
    value.var = c('data_original', 'correction_input', 'correction_type', 'correction_metadata')
  )

  complete_with_corrections <- left_join(
    data,
    corrections,
    by = c(
      'geographicAreaM49Reporter' = 'reporter',
      'geographicAreaM49Partner'  = 'partner',
      'measuredItemCPC'           = 'item',
      'flow'
      )
    ) %>%
    filter(!is.na(correction_input_qty) | !is.na(correction_input_value)) %>%
    # XXX mirror?
    mutate(
      correction_drop =
                       ifelse(!is.na(correction_input_qty)   & (qty   < 0.99*data_original_qty   | qty   > 1.01*data_original_qty),   TRUE, FALSE) |
                       ifelse(!is.na(correction_input_value) & (value < 0.99*data_original_value | value > 1.01*data_original_value), TRUE, FALSE)
    )

  corrections_to_drop <- filter(complete_with_corrections, correction_drop)

  complete_with_corrections <- complete_with_corrections %>%
    filter(!correction_drop) %>%
    select(-correction_drop) %>%
    mutate(
      qty                     = ifelse(!is.na(correction_input_qty),   correction_input_qty,   qty),
      value                   = ifelse(!is.na(correction_input_value), correction_input_value, value),
      uv                      = ifelse(qty > 0, value * 1000 / qty, NA),
      flagObservationStatus_q = ifelse(
                                  !is.na(correction_input_qty),
                                  ifelse(
                                    correction_type_qty != 'None',
                                    ifelse(correction_type_qty == 'Mirror flow', 'T', 'I'),
                                    flagObservationStatus_q
                                  ),
                                  flagObservationStatus_q
                                ),
      flagObservationStatus_v = ifelse(
                                  !is.na(correction_input_value),
                                  ifelse(
                                    correction_type_value != 'None',
                                    ifelse(correction_type_value == 'Mirror flow', 'T', 'I'),
                                    flagObservationStatus_v
                                  ),
                                  flagObservationStatus_v
                                ),
      flagMethod_q            = ifelse(
                                  !is.na(correction_input_qty),
                                  ifelse(correction_type_qty != 'None', 'e', flagMethod_q),
                                  flagMethod_q
                                ),
      flagMethod_v            = ifelse(
                                  !is.na(correction_input_value),
                                  ifelse(correction_type_value != 'None', 'e', flagMethod_v),
                                  flagMethod_v
                                )
    ) %>%
    select(
      -data_original_qty,   -correction_input_qty,   -correction_type_qty,
      -data_original_value, -correction_input_value, -correction_type_value
    )

    return(
      list(
        to_drop   = corrections_to_drop,
        corrected = complete_with_corrections
      )
    )
}

