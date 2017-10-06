#' Looks for corresponding FCL codes in country-specific
#' mapping tables from MDB files
#'
#' @param uniqhs Data frame with columns reporter, flow, hsext.
#' @param maptable Data frame with HS->FCL mapping with columns area,
#'   flow, fromcode, tocode, fcl
#' @param parallel Logical. Should multicore backend be used.
#'
#' @return Data frame with columns reporter, flow, datumid, hs, hsext, fcl.
#'   datumid holds row numbers of original dataset. hs is input hs. hsext is
#'   input hs with additional zeros if requires. If there are multiple
#'   HS->FCL matchings, all of them are returned with similar id. If
#'   there were no matching FCL codes, NA in fcl column is returned.
#'
#' @import dplyr
#' @import data.table
#' @export


hsInRange <- function(uniqhs, maptable, parallel = FALSE) {

  # Looking for matching fcl codes for each input hs code.
  # If there are multiple matchings we return all matches.
  # dataframes need to be transformed to data.table format
  # and the RHS data.table needs keys.

  df <- uniqhs %>%
    dplyr::mutate(datumid = row_number()) %>%
    dplyr::mutate(fromcodeext = hsext, tocodeext = hsext) %>%
    as.data.table()

  maptable <- as.data.table(maptable)
  
  setkey(maptable, reporter, flow, fromcodeext, tocodeext)

  foverlaps(df, maptable) %>%
    tbl_df() %>%
    select(reporter, flow, datumid, hs, hsext, fcl, recordnumb)

}
