#' Report the number of records with missing quantity/weight.
#'
#' @inherit Params ts_all_reports
#'
#' @import dplyr
#'
ts_missing_records <- function(collection_path = NULL, prefix = NULL) {

  elems <- c("esdata_rawdata_nonmrc",
             "tldata_rawdata_nonmrc")

  extract_rprt_elem(collection_path, prefix, elems) %>%
    bind_rows() %>%
    select(reporter, name, year, flow, noqty, noqty_prop) %>%
    mutate(flow = recode(
                    flow,
                    `1` = 'import',
                    `2` = 'export',
                    `3` = 'reexport',
                    `4` = 'reimport'
       )) %>%
    arrange(reporter, name, flow, year) %>%
    data.table::as.data.table() %>%
    data.table::dcast.data.table(
      reporter + name + year ~ flow,
      value.var = c("noqty", "noqty_prop")
    ) %>%
    rename_(rep_code = ~ reporter,
            rep_name = ~ name)
}
