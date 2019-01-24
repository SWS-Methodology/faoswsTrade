#' Aggregate multiple rows in raw Tariff Line data.
#'
#' Do a first aggregation of raw Tariff Line data, when possible.
#'
#' Tariff Line (TL) data can have multiple rows and can be aggregated
#' (by taking also into account the possible different units) when value,
#' quantity, and weight are strictly greater than zero. Other possibilities
#' is to do the aggregation when weight is always missing/zero, but
#' quantity is always present and vice versa. When aggregation is not
#' possible the record will be left untouched as the missing/zero variable
#' will be imputed in the imputation step.
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
    mutate_(
      no_quant    = ~is.na(qty),
      no_weight   = ~is.na(weight),
      zero_quant  = ~near(qty, 0),
      zero_weight = ~near(weight, 0),
      nrows = 1
    )

  raw %>%
    group_by_(
      ~year, ~reporter, ~partner, ~flow, ~hs6, ~hs, ~qunit,
      ~no_quant, ~no_weight, ~zero_quant, ~zero_weight
    ) %>%
    summarise_each_(
      funs(sum(.)),
      vars = c('value', 'weight', 'qty', 'nrows')
    ) %>%
    ungroup()
}
