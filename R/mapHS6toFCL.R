#' Map HS to FCL based on HS codes of length 6
#'
#' @param tradedata Trade data frame.
#' @param hs6maptable Map table produced by \code{extract_hs6fclmap} function.
#' @param parallel Should parallel execution be used. \code{FALSE} by default.
#'
#' @return Data frame with unique reporter/flow/hs->fcl links.
#'
#' @import dplyr
#' @import stringr
#' @export

mapHS6toFCL <- function(tradedata, hs6maptable, parallel = FALSE) {

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

  tradedata %>%
    inner_join(hs6maptable, by = c("reporter", "flow", "hs6"))
}
