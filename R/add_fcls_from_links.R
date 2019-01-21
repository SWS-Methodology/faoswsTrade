#' Add fcl codes to trade data from links datasets
#'
#' @param tradedata Data.
#' @param hs6links HS6 links.
#' @param links HS-FCL links.
#'
#' @import dplyr
#' @export

add_fcls_from_links <- function(tradedata, hs6links, links) {

  # NOTE: The original implementation is commented below. It
  # seems to be equivalent to the new implementation, after
  # the commented original code. Probably there was a rationale
  # for the original implementation, but at this moment it is
  # not clear. It is kept for reference.
  #
  ## sel_notna <- function(a, b) {
  ##   stopifnot(length(a) == length(b))
  ##   ab <- matrix(c(a, b), ncol = 2)
  ##   apply(ab, 1L, function(i) {
  ##     notna <- !is.na(i)
  ##     stopifnot(sum(notna) < 2)
  ##     res <- i[notna]
  ##     ifelse(length(res) == 1L, res, NA)
  ##   })
  ## }

  ## tradedatahs6 <- tradedata %>%
  ##   left_join(hs6links, by = c("reporter", "flow", "hs6"))

  ## tradedata <- tradedata %>%
  ##   left_join(links, by = c("reporter", "flow", "hs"))

  ## tradedata$fcl_hs6 <- tradedatahs6$fcl
  ## tradedata <- tradedata %>%
  ##   mutate_(fcl = ~sel_notna(fcl, fcl_hs6)) %>%
  ##   select_(~-fcl_hs6)

  left_join(
    tradedata,
    dplyr::rename(hs6links, fcl6 = fcl),
    by = c("reporter", "flow", "hs6")
  ) %>%
  left_join(
    dplyr::rename(links, fclx = fcl),
    by = c("reporter", "flow", "hs")
  ) %>%
  dplyr::mutate(fcl = ifelse(is.na(fclx), fcl6, fclx)) %>%
  dplyr::select(-fclx, -fcl6)

}
