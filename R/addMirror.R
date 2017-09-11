#' Add mirror data.
#'
#' @param data data.
#'
#' @return Data with mirror.
#'
#' @import dplyr
#'
#' @export

addMirror <- function(data = NA) {
  data_mirror <- data %>%
    rename(
           geographicAreaM49Reporter = geographicAreaM49Partner,
           geographicAreaM49Partner  = geographicAreaM49Reporter
           ) %>%
    mutate(
            flow  = recode(flow, '2' = 1, '1' = 2),
            value = ifelse(flow == 1, value*1.12, value/1.12)
           ) %>%
    select(
           timePointYears,
           geographicAreaM49Reporter,
           geographicAreaM49Partner,
           flow,
           cpc,
           qty,
           value,
           flag_qty,
           flag_value
           )

  res <- full_join(data,
                   data_mirror,
                   by = c(
                          'timePointYears',
                          'geographicAreaM49Reporter',
                          'geographicAreaM49Partner',
                          'flow',
                          'measuredItemCPC'
                          ),
                   suffix = c('_r', '_m')
                   ) %>%
    rename(
           qty = qty_r,
           value = value_r,
           flag_qty = flag_qty_r,
           flag_value = flag_value_r
           ) %>%
    mutate(ratio_mirror = qty/qty_m)

  return(res)
}
