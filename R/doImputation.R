#' Impute missing values or outliers.
#'
#' Use a median unit value to impute missing quantities or outliers.
#'
#' The median unit value is calculated in a specific-to-generic fashion:
#' 1) by HS-reporter; 2) by HS8-reporter; by HS6-reporter; 3) by HS;
#' 4) by FCL; 5) by flow.
#' The first available of these options will be used (for HS, HS8 and HS6
#' only when there are at least 10 flows).
#'
#' @param tradedata Trade data.
#' @return \code{tradedata} with imputed values.
#' @import dplyr
#' @export

doImputation <- function(tradedata = NA) {

  if (missing(tradedata)) stop('"tradedata" is missing.')

  tradedata <- tradedata %>%
    mutate(i=1, hs8 = str_sub(hs, 1, 8), hs6 = str_sub(hs, 1, 6)) %>%
    mutate(
           hs8 = if_else(nchar(hs8)==7, paste0('0', hs8), hs8),
           hs6 = if_else(nchar(hs6)==5, paste0('0', hs6), hs6)
           ) %>%
    # median value reporter/HS
    group_by_(~year, ~reporter, ~flow, ~hs) %>%
    mutate_(n_reporter_hs = ~sum(i), uv_reporter_hs = ~median(uv, na.rm = TRUE)) %>%
    ungroup() %>%
    # median value reporter/HS8
    group_by_(~year, ~reporter, ~flow, ~hs8) %>%
    mutate_(n_reporter_hs8 = ~sum(i), uv_reporter_hs8 = ~median(uv, na.rm = TRUE)) %>%
    ungroup() %>%
    # median value reporter/HS6
    group_by_(~year, ~reporter, ~flow, ~hs6) %>%
    mutate_(n_reporter_hs6 = ~sum(i), uv_reporter_hs6 = ~median(uv, na.rm = TRUE)) %>%
    ungroup() %>%
    # median value HS
    group_by_(~year, ~flow, ~hs) %>%
    mutate_(n_hs = ~sum(i), uv_hs = ~median(uv, na.rm = TRUE)) %>%
    ungroup() %>%
    # median value FCL
    group_by_(~year, ~flow, ~fcl) %>%
    mutate_(uv_fcl = ~median(uv, na.rm = TRUE)) %>%
    ungroup() %>%
    # median value flow
    group_by_(~year, ~flow) %>%
    mutate_(uv_flow = ~median(uv, na.rm = TRUE)) %>%
    ungroup()

  imputed_qty <- case_when(
    !is.na(tradedata$uv_reporter_hs)  & tradedata$n_reporter_hs  > 10  ~ tradedata$value / tradedata$uv_reporter_hs,
    !is.na(tradedata$uv_reporter_hs8) & tradedata$n_reporter_hs8 > 10  ~ tradedata$value / tradedata$uv_reporter_hs8,
    !is.na(tradedata$uv_reporter_hs6) & tradedata$n_reporter_hs6 > 10  ~ tradedata$value / tradedata$uv_reporter_hs6,
    !is.na(tradedata$uv_hs)                                            ~ tradedata$value / tradedata$uv_hs,
    !is.na(tradedata$uv_fcl)                                           ~ tradedata$value / tradedata$uv_fcl,
    # XXX
    !is.na(tradedata$uv_flow)                                          ~ tradedata$value / tradedata$uv_flow
  )

  tradedata %>%
    mutate_(qty = ~ifelse(no_quant | outlier, imputed_qty, qty),
           flagTrade = ~ifelse(no_quant | outlier, 1, 0))

}
