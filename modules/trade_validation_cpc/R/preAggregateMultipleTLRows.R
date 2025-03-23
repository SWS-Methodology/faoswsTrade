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
#' @import data.table
#' @export

preAggregateMultipleTLRows <- function(rawdata = NA) {

  if (missing(rawdata)) stop('"rawdata" is required')

  rawdata[,
    `:=`(
      imputed_puv  = FALSE,
      imputed_qbyw = FALSE,
      no_weight    = is.na(weight),
      zero_weight  = near(weight, 0)
    )
  ]

  rawdata[,
    `:=`(
      value_nonna  = sum(value[!(no_weight | zero_weight)]),
      weight_nonna = sum(weight[!(no_weight | zero_weight)])
    ),
    .(year, reporter, partner, flow, hs)
  ]

  rawdata[,
    perc_nonna := value_nonna / sum(value, na.rm = TRUE),
    .(year, reporter, partner, flow, hs)
  ]

  rawdata[, uv_nonna := value_nonna / weight_nonna]

  rawdata[
    (no_weight == TRUE | zero_weight == TRUE) & !is.na(uv_nonna) & perc_nonna > 0.5,
    `:=`(
      # Imputed with partially reported unit value
      weight      = value / uv_nonna,
      imputed_puv = TRUE
    )
  ]

  rawdata[
    qunit == 1 & !is.na(weight) & weight > 0,
    `:=`(
      qty          = weight,
      qunit        = 8,
      imputed_qbyw = TRUE
    )
  ]

  rawdata[,
    `:=`(
      no_quant    = is.na(qty),
      no_weight   = is.na(weight),
      zero_quant  = near(qty, 0),
      zero_weight = near(weight, 0),
      nrows       = 1L
    )
  ]

  rawdata[,
    lapply(.SD, sum),
    list(year, reporter, partner, flow, hs6, hs, qunit,
         no_quant, no_weight, zero_quant, zero_weight),
    .SDcols = c('value', 'weight', 'qty', 'nrows', 'imputed_puv', 'imputed_qbyw')
  ]
}
