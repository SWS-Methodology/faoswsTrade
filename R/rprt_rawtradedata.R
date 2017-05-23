#' Report table on raw trade data
#'
#'@import dplyr
#'@import scales

rprt_rawtradedata <- function(tradedata = NULL, tradedataname = NULL) {

  stopifnot(!is.null(tradedata))
  stopifnot(!is.null(tradedataname))

  # Add area name
  if (tradedataname %in% c("esdata", "tldata")) {
    area_codes_class <- if_else(tradedataname == "esdata", "geonom", "m49")
    tradedata <- add_area_names(tradedata, area_codes_class, "reporter")
  } else warning("No suitable area code list for given trade data name.")


  # Report non numeric partner and hs codes summary
  rawdata_nonmrc <- tradedata %>%
    group_by_(~reporter, ~name, ~flow) %>%
    summarize_(nonmrc_prt = ~sum(partner_non_numeric),
               nonmrc_hs = ~sum(hs_non_numeric),
               nonmrc_prt_prop = ~nonmrc_prt / n(),
               nonmrc_hs_prop = ~nonmrc_hs / n()) %>%
    ungroup()

  rprt_writetable(rawdata_nonmrc, prefix = tradedataname)

  # Report length of hs code
  rawdata_hslength <- tradedata %>%
    select_(~reporter, ~name, ~flow, ~hs) %>%
    mutate_(hslength = ~stringr::str_length(hs)) %>%
    group_by_(~reporter, ~name, ~flow, ~hslength) %>%
    summarize_(count = ~n()) %>%
    ungroup() %>%
    tidyr::spread_("hslength", "count", fill = 0L)

  rprt_writetable(rawdata_hslength, prefix = tradedataname)
}
