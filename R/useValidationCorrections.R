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

  corrections <- corrections %>%
    dplyr::mutate(correction_id = 1:n()) %>%
    as.data.table()

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
    value.var = c('data_original', 'correction_input', 'correction_type', 'correction_metadata', 'correction_id')
  )

  if (!('data_original_value' %in% names(corrections))) {
    for (i in corrections %>% select(ends_with('qty')) %>% names()) {
      corrections[[sub('_qty', '_value', i)]] = NA
    }
  }

  if (!('data_original_qty' %in% names(corrections))) {
    for (i in corrections %>% select(ends_with('value')) %>% names()) {
      corrections[[sub('_value', '_qty', i)]] = NA
    }
  }

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
    dplyr::filter(!is.na(correction_input_qty) | !is.na(correction_input_value)) %>%
    # XXX mirror?
    dplyr::mutate(
      x_qty   = !is.na(correction_input_qty),
      x_value = !is.na(correction_input_value),
      y_qty   = (qty   < 0.99 * data_original_qty   | qty   > 1.01 * data_original_qty),
      y_value = (value < 0.99 * data_original_value | value > 1.01 * data_original_value),
      correction_qty_apply    = x_qty & !y_qty,
      correction_qty_apply    = ifelse(is.na(!x_qty & y_qty), NA, correction_qty_apply),
      correction_value_apply  = x_value & !y_value,
      correction_value_apply  = ifelse(is.na(!x_value & y_value), NA,    correction_value_apply),
      qty                     = ifelse(correction_qty_apply   %in% TRUE, correction_input_qty, qty),
      value                   = ifelse(correction_value_apply %in% TRUE, correction_input_value, value),
      uv                      = ifelse(qty > 0, value * 1000 / qty, NA),
      flagObservationStatus_q = ifelse(
                                  correction_qty_apply %in% TRUE,
                                  ifelse(
                                    correction_type_qty != 'None',
                                    ifelse(correction_type_qty == 'Mirror flow', 'T', 'I'),
                                    flagObservationStatus_q
                                  ),
                                  flagObservationStatus_q
                                ),
      flagObservationStatus_v = ifelse(
                                  correction_value_apply %in% TRUE,
                                  ifelse(
                                    correction_type_value != 'None',
                                    ifelse(correction_type_value == 'Mirror flow', 'T', 'I'),
                                    flagObservationStatus_v
                                  ),
                                  flagObservationStatus_v
                                ),
      flagMethod_q            = ifelse(
                                  correction_qty_apply %in% TRUE,
                                  ifelse(correction_type_qty != 'None', 'e', flagMethod_q),
                                  flagMethod_q
                                ),
      flagMethod_v            = ifelse(
                                  correction_value_apply %in% TRUE,
                                  ifelse(correction_type_value != 'None', 'e', flagMethod_v),
                                  flagMethod_v
                                )
    )

  corrections_drop_qty <- complete_with_corrections$correction_id_qty[complete_with_corrections$correction_qty_apply %in% FALSE]

  corrections_drop_value <- complete_with_corrections$correction_id_value[complete_with_corrections$correction_value_apply %in% FALSE]

  corrections_to_drop <- corrections_table[c(corrections_drop_qty, corrections_drop_value),]

  complete_with_corrections <- complete_with_corrections %>%
    select(
      geographicAreaM49Reporter,
      geographicAreaM49Partner,
      flow,
      timePointYears,
      flagObservationStatus_v,
      flagObservationStatus_q,
      flagMethod_v,
      flagMethod_q,
      measuredItemCPC,
      qty,
      unit,
      value,
      uv,
      correction_metadata_qty,
      correction_metadata_value
    )


    return(
      list(
        to_drop   = corrections_to_drop,
        corrected = complete_with_corrections
      )
    )
}

