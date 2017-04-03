#' Select one record from multiple matches during HS->FCL mapping process.
#'
#' @param hsfclmatch Data frame with columns reporter, flow, hs, hsext, fcl,
#' datumid, maplinkid
#' @param maptable Data frame with HS->FCL mapping table.
#'
#' @return Data frame similar to hsfclmatch but with all duplicates removed.
#'
#' @export
#' @import dplyr
#'

sel1FCL <- function(hsfclmatch, maptable) {

  if(length(unique(hsfclmatch$datumid)) > 1L) return(
    hsfclmatch %>%
      group_by_(~datumid) %>%
      do(sel1FCL(., maptable)) %>%
      ungroup
  )

  # Return original record if there is only one match
  if(nrow(hsfclmatch) == 1L) return(hsfclmatch)

  # Selection of the narrowest hs range
  hsfclmatch <- hsfclmatch %>%
    left_join(maptable %>%
                select_(~recordnumb,
                        ~fromcodeext,
                        ~tocodeext),
              by = "recordnumb") %>%
    mutate_(hsrange = ~tocodeext - fromcodeext) %>%
    filter_(~hsrange == min(hsrange))

  # If there are still several options
  # we choose the yougest
  if(nrow(hsfclmatch) > 1L) {
    hsfclmatch <- hsfclmatch %>%
      filter_(~recordnumb == max(recordnumb))
  }

  stopifnot(nrow(hsfclmatch) == 1L)

  hsfclmatch %>%
    select_(~reporter, ~flow, ~datumid, ~hs, ~hsext, ~fcl, ~recordnumb)

}
