#' Report table on raw Eurostat data
#'
#'@import dplyr
#'@import scales
#'@export

rprt_raw_eurostat <- function(esdata) {

  esdata <- esdata %>%
    mutate_at(vars(declarant, partner),
              funs(non_numeric = !grepl("^[[:digit:]]+$", .))) %T>%
              {flog.info("Non-numeric area codes: ",
                         summarize_at(.,
                                      .cols = vars(ends_with("non_numeric")),
                                      .funs = funs(total = sum,
                                                   prop = percent(sum(.) / n()))),
                         capture = TRUE)} %>%
    filter_(~!declarant_non_numeric & !partner_non_numeric) %>%
    select(-ends_with("non_numeric"))
  

}
