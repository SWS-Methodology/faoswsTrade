#' Add fcl codes to trade data from links datasets
#'
#' @import dplyr
#' @export

add_fcls_from_links <- function(tradedata, hs6links, links) {

  sel_notna <- function(a, b) {
    stopifnot(length(a) == length(b))
    ab <- matrix(c(a, b), ncol = 2)
    apply(ab, 1L, function(i) {
      notna <- !is.na(i)
      stopifnot(sum(notna) < 2)
      res <- i[notna]
      ifelse(length(res) == 1L, res, NA)
    })
  }

  tradedatahs6 <- tradedata %>%
    left_join(hs6links, by = c("reporter", "flow", "hs6"))

  tradedata <- tradedata %>%
    left_join(links, by = c("reporter", "flow", "hs"))

  tradedata$fcl_hs6 <- tradedatahs6$fcl
  tradedata <- tradedata %>%
    mutate_(fcl = ~sel_notna(fcl, fcl_hs6)) %>%
    select_(~-fcl_hs6)

}
