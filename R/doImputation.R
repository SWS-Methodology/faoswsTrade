#' Impute missing values or outliers.
#'
#' Use a median unit value to impute missing quantities or outliers.
#'
#' The median unit value is calculated in a specific-to-generic fashion:
#' 1) by HS-reporter; 2) by FCL-reporter; 3) by HS; 4) by FCL; 5) by flow.
#' The first available of these options will be used.
#'
#' @param tradedata Trade data.
#' @return \code{tradedata} with imputed values.
#' @import dplyr
#' @export

doImputation <- function(tradedata = NA) {

  if (missing(tradedata)) stop('"tradedata" is missing.')

  tradedata <- tradedata %>%
    # median value reporter/HS
    group_by_(~year, ~reporter, ~flow, ~hs) %>%
    mutate_(uv_reporter_hs = ~median(uv, na.rm = TRUE)) %>%
    ungroup() %>%
    # median value reporter/FCL
    group_by_(~year, ~reporter, ~flow, ~fcl) %>%
    mutate_(uv_reporter_fcl = ~median(uv, na.rm = TRUE)) %>%
    ungroup() %>%
    # median value HS
    group_by_(~year, ~flow, ~hs) %>%
    mutate_(uv_hs = ~median(uv, na.rm = TRUE)) %>%
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
    !is.na(tradedata$uv_reporter_hs)  ~ tradedata$value / tradedata$uv_reporter_hs,
    !is.na(tradedata$uv_reporter_fcl) ~ tradedata$value / tradedata$uv_reporter_fcl,
    !is.na(tradedata$uv_hs)           ~ tradedata$value / tradedata$uv_hs,
    !is.na(tradedata$uv_fcl)          ~ tradedata$value / tradedata$uv_fcl,
    # XXX
    !is.na(tradedata$uv_flow)         ~ tradedata$value / tradedata$uv_flow
  )

  tradedata <- tradedata %>%
    mutate_(qty = ~ifelse(no_quant | outlier, imputed_qty, qty),
           flagTrade = ~ifelse(no_quant | outlier, 1, 0))

}

