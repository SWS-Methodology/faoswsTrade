#' Aggregate multiple rows in raw Tariff Line data.
#'
#' Do a first aggregation of raw Tariff Line data, when possible.
#'
#' Tariff Line (TL) data can have multiple rows. They can be aggregated
#' (by taking also into account the possible different units) when value,
#' quantity, and weight are present. Other two possibilities is to do the
#' aggregation when weight is always missing, but quantity is always present
#' and vice versa. When aggregation is not possible the record will be left
#' untouched as the missing variable will be imputed in the imputation step.
#'
#' @param rawdata Raw TL (a tibble).
#' @return The original TL data after aggregation, when possible (see
#'   details). A new variable (\code{nrows}) is created and indicates the
#'   original number of rows that were aggregated.
#' @import dplyr
#' @export

preAggregateMultipleTLRows <- function(rawdata = NA) {

  if (missing(rawdata)) stop('"rawdata" is required')

  raw <- rawdata %>%
    mutate_(no_quant  = ~is.na(qty),
            no_weight = ~is.na(weight),
            nrows = 1)

  raw$cases <- case_when(
                         !raw$no_quant & !raw$no_weight ~ 1L,
                         !raw$no_quant &  raw$no_weight ~ 2L,
                          raw$no_quant & !raw$no_weight ~ 3L,
                          raw$no_quant &  raw$no_weight ~ 4L
                         )
  raw %>%
        group_by_(~tyear, ~rep, ~prt, ~flow, ~comm, ~qunit, ~cases) %>%
        summarise_each_(
                        funs(sum(.)),
                        vars = c('weight', 'qty', 'tvalue', 'nrows')) %>%
        ungroup() %>%
        select_(~-cases)
}
