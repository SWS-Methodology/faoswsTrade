#' Select one record from multiple matches during HS->CPC mapping process.
#'
#' @param hscpcmatch Data frame with columns reporter, flow, hs, hsext, cpc,
#' datumid, maplinkid
#' @param maptable Data frame with HS->CPC mapping table.
#' @param cur_yr Integer, year the trade module run on.
#'
#' @return Data frame similar to hscpcmatch but with all duplicates removed.
#'
#' @export
#' @import dplyr
#'

sel1CPC <- function(hscpcmatch, maptable, cur_yr = NULL) {

  stopifnot(!is.null(cur_yr))
  stopifnot(is.integer(cur_yr))

  # Choose records suitable by start/end year values in the mapping table
  # Firstly calculate distance to years interval:
  # 0 if the record is inside of interval
  # positive integer (number of years to the closest interval border)

  hscpcmatch <- hscpcmatch %>%
    left_join(maptable %>%
                select_(~startyear,
                        ~endyear,
                        ~recordnumb,
                        ~fromcodeext,
                        ~tocodeext),
              by = "recordnumb") %>%
    mutate_(hsrange = ~tocodeext - fromcodeext) %>%
    mutate_(inrange = ~startyear <= cur_yr & endyear >= cur_yr,
            rangedist = ~pmin.int(abs(startyear - cur_yr),
                                  abs(endyear - cur_yr)),
            rangedist = ~ifelse(inrange, 0, rangedist)) %>%
    group_by(datumid) %>%
    filter_(~rangedist == min(rangedist)) %>%
    # If there are still several options choose narrowest hs range
    filter_(~hsrange == min(hsrange)) %>%
    # If there are still several options choose the yougest
    filter_(~recordnumb == max(recordnumb)) %>%
    ungroup()

  hscpcmatch %>%
    select_(~reporter, ~flow, ~datumid, ~hs, ~hsext, ~cpc, ~fcl, ~recordnumb)

}

