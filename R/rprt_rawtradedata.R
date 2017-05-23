#' Report table on raw trade data
#'
#'@import dplyr
#'@import scales

rprt_rawtradedata <- function(tradedata = NULL, tradedataname = NULL) {

  stopifnot(!is.null(tradedata))
  stopifnot(!is.null(tradedataname))

  rawdata_statistics <- tradedata %>%
    group_by_(~reporter, ~flow) %>%
    summarize_(nonmrc_prt = ~sum(partner_non_numeric),
               nonmrc_hs = ~sum(hs_non_numeric),
               nonmrc_prt_prop = ~nonmrc_prt / n(),
               nonmrc_hs_prop = ~nonmrc_hs / n()) %>%
    ungroup()

  rprt_writetable(rawdata_statistics, prefix = tradedataname)

}
