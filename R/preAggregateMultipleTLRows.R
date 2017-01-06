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

  # TODO (Christian) there are some countries that have
  # a non-unique length of HS:
  # s <- table(tldata$rep, nchar(tldata$comm))
  # sort(apply(s!=0, 1, sum))

  if (missing(rawdata)) stop('"rawdata" is required')

  rawdata_orig <- rawdata %>%
    tbl_df() %>%
    select(-chapter) %>%
    mutate(no_quant = is.na(qty),
            no_weight = is.na(weight))

  step1 <- rawdata_orig %>%
        filter(!no_quant, !no_weight) %>%
        group_by(tyear, rep, prt, flow, comm, qunit) %>%
        summarise_each(
                        funs(sum(.)),
                        vars = c(weight, qty, tvalue)) %>%
        ungroup()

  step2 <- rawdata_orig %>%
        filter(no_quant, !no_weight) %>%
        group_by(tyear, rep, prt, flow, comm, qunit) %>%
        summarise_each(
                        funs(sum(.)),
                        vars = c(weight, qty, tvalue)) %>%
        ungroup()

  step3 <- rawdata_orig %>%
        filter(!no_quant, no_weight) %>%
        group_by(tyear, rep, prt, flow, comm, qunit) %>%
        summarise_each(
                        funs(sum(.)),
                        vars = c(weight, qty, tvalue)) %>%
        ungroup()

  step4 <- rawdata_orig %>%
        filter(no_quant, no_weight) %>%
        group_by(tyear, rep, prt, flow, comm, qunit) %>%
        summarise_each(
                        funs(sum(.)),
                        vars = c(weight, qty, tvalue)) %>%
        ungroup()
      

  bind_rows(step1, step2, step3, step4)
}
