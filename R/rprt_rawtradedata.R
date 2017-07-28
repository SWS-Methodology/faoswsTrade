#' Report table on raw trade data
#'
#'@import dplyr
#'@import scales

rprt_rawtradedata <- function(tradedata = NULL, tradedataname = NULL) {

  stopifnot(!is.null(tradedata))
  stopifnot(!is.null(tradedataname))

  # Add area name
  if (tradedataname %in% c("esdata", "tldata")) {
    area_codes_class <- if_else(tradedataname == "esdata", "geonom", "fao")
    tradedata <- add_area_names(tradedata, area_codes_class, "reporter")
  } else warning("No suitable area code list for given trade data name.")

  # Non numeric partners
  rawdata_nonmrc_prtnrs_full <- tradedata %>%
    filter_(~partner_non_numeric) %>%
    select_(~-ends_with("non_numeric"))

  rprt_writetable(rawdata_nonmrc_prtnrs_full, prefix = tradedataname,
                  subdir = "preproc")


  # Non numeric hs codes
  rawdata_nonmrc_hs_full <- tradedata %>%
    filter_(~hs_non_numeric) %>%
    select_(~-ends_with("non_numeric"))

  rprt_writetable(rawdata_nonmrc_hs_full, prefix = tradedataname,
                  subdir = "preproc")


  # Report non numeric partner, hs codes summary, missing
  # value and quantity
  rawdata_nonmrc <- tradedata %>%
    mutate_(hslength = ~stringr::str_length(hs)) %>%
    group_by_(~reporter, ~name, ~flow) %>%
    summarize_(records_count = ~n(),
               partners = ~length(unique(partner)),
               nonmrc_prt = ~sum(partner_non_numeric),
               nonmrc_hs = ~sum(hs_non_numeric),
               novalue = ~sum(novalue),
               noqty = ~sum(noqty),
               hslength = ~max(hslength)) %>%
    ungroup() %>%
    mutate_at(vars(nonmrc_prt, nonmrc_hs, novalue, noqty),
              funs(prop = . / records_count))

  rprt_writetable(rawdata_nonmrc, prefix = tradedataname, subdir = "preproc")


  # Report length of hs code
  rawdata_hslength <- tradedata %>%
    select_(~reporter, ~name, ~flow, ~hs) %>%
    mutate_(hslength = ~stringr::str_length(hs)) %>%
    group_by_(~reporter, ~name, ~flow, ~hslength) %>%
    summarize_(count = ~n()) %>%
    ungroup() %>%
    tidyr::spread_("hslength", "count", fill = 0L)

  rprt_writetable(rawdata_hslength, prefix = tradedataname, subdir = "preproc")
}
