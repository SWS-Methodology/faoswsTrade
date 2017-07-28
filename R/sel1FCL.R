#' Select one record from multiple matches during HS->FCL mapping process.
#'
#' @param hsfclmatch Data frame with columns reporter, flow, hs, hsext, fcl,
#' datumid, maplinkid
#' @param maptable Data frame with HS->FCL mapping table.
#' @param cur_yr Integer, year the trade module run on.
#'
#' @return Data frame similar to hsfclmatch but with all duplicates removed.
#'
#' @export
#' @import dplyr
#'

sel1FCL <- function(hsfclmatch, maptable, cur_yr = NULL) {

  stopifnot(!is.null(cur_yr))
  stopifnot(is.integer(cur_yr))

  if(length(unique(hsfclmatch$datumid)) > 1L) return(
    hsfclmatch %>%
      group_by_(~datumid) %>%
      do(sel1FCL(., maptable, cur_yr)) %>%
      ungroup
  )

  # Return original record if there is no multiple matches
  if(nrow(hsfclmatch) == 1L) return(hsfclmatch)

  # Choose records suitable by start/end year values in the mapping table
  # Firstly calculate distance to years interval:
  # 0 if the record is inside of interval
  # positive integer (number of years to the closest interval border)

  hsfclmatch <- hsfclmatch %>%
    left_join(maptable %>%
                select_(~startyear,
                        ~endyear,
                        ~recordnumb,
                        ~fromcodeext,
                        ~tocodeext),
              by = "recordnumb")

  hsfclmatch <- hsfclmatch %>%
    mutate_(inrange = ~startyear <= cur_yr & endyear >= cur_yr,
            rangedist = ~pmin.int(abs(startyear - cur_yr),
                                  abs(endyear - cur_yr)),
            rangedist = ~ifelse(inrange, 0, rangedist)) %>%
    filter_(~rangedist == min(rangedist))

  if(nrow(hsfclmatch) > 1L) {
  # Selection of the narrowest hs range
  hsfclmatch <- hsfclmatch %>%
    mutate_(hsrange = ~tocodeext - fromcodeext) %>%
    filter_(~hsrange == min(hsrange))
  }

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
