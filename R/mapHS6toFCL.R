#' Map HS to FCL based on HS codes of length 6
#'
#' @param tradedata Trade data frame.
#' @param hs6maptable Map table produced by \code{extract_hs6fclmap} function.
#'
#' @return Data frame with unique reporter/flow/hs->fcl links.
#'
#' @import dplyr
#' @import stringr
#' @export

mapHS6toFCL <- function(tradedata, hs6maptable) {

  # Name for passing to reporting functions
  tradedataname <- lazyeval::expr_text(tradedata)

  # HS6 mapping table subset with 1-to-1 hs->fcl links
  hs6maptable <- hs6maptable %>%
    filter_(~fcl_links == 1L) %>%
    distinct_(~reporter, ~flow, ~hs6, ~fcl)

  if(!"hs6" %in% colnames(tradedata)) {
    tradedata <- tradedata %>%
      mutate_(hs6 = ~str_sub(hs, end = 6L))
  }

  if(!is.integer(tradedata$hs6)) {
    tradedata <- tradedata %>%
      mutate_(hs6 = ~as.integer(hs6))
  }

  tradedata <- tradedata %>%
    select_(~reporter, ~flow, ~hs6) %>%
    distinct()

  rowsbeforejoin <- nrow(tradedata)

  tradedata <- tradedata %>%
    left_join(hs6maptable, by = c("reporter", "flow", "hs6"))

  if(rowsbeforejoin != nrow(tradedata))
    warning(paste0("Rows before join: ", rowsbeforejoin,
                   ", after join: ", nrow(tradedata)))

  # XXX: bring back
  #rprt(tradedata, "hs6fcl_results", tradedataname = tradedataname)

  tradedata %>%
    filter_(~!is.na(fcl))
}
