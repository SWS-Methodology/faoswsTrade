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
#'   details).
#' @import dplyr
#' @export

preAggregateMultipleTLRows <- function(rawdata = NA) {

  if (missing(rawdata)) stop('"rawdata" is required')

  rawdata_orig <- tldata %>%
    tbl_df() %>%
    select_(~-chapter) %>%
    # qty and weight seem to be always >0 or NA
    # no missing value
    mutate_(no_quant = ~is.na(qty),
            no_weight = ~is.na(weight))

  step1 <- rawdata_orig %>%
        filter_(~(!no_quant & !no_weight)) %>%
        group_by_(~tyear, ~rep, ~prt, ~flow, ~comm, ~qunit) %>%
        summarise_each_(
                        funs(sum(.)),
                        vars = c("weight", "qty", "tvalue")) %>%
        ungroup()

  step2 <- rawdata_orig %>%
        filter_(~(no_quant & !no_weight)) %>%
        group_by_(~tyear, ~rep, ~prt, ~flow, ~comm, ~qunit) %>%
        summarise_each_(
                        funs(sum(.)),
                        vars = c("weight", "qty", "tvalue")) %>%
        ungroup()

  step3 <- rawdata_orig %>%
        filter_(~(!no_quant & no_weight)) %>%
        group_by_(~tyear, ~rep, ~prt, ~flow, ~comm, ~qunit) %>%
        summarise_each_(
                        funs(sum(.)),
                        vars = c("weight", "qty", "tvalue")) %>%
        ungroup()

  step4 <- rawdata_orig %>%
        filter_(~(no_quant & no_weight)) %>%
        group_by_(~tyear, ~rep, ~prt, ~flow, ~comm, ~qunit) %>%
        summarise_each_(
                        funs(sum(.)),
                        vars = c("weight", "qty", "tvalue")) %>%
        ungroup()
      

  bind_rows(step1, step2, step3, step4)
}
