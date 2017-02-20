#' Looks for corresponding FCL codes in country-specific
#' mapping tables from MDB files
#'
#' @param uniqhs Data frame with columns reporter, flow, hs.
#' @param maptable Data frame with HS->FCL mapping with columns area,
#'   flow, fromcode, tocode, fcl
#' @param parallel Logical. Should multicore backend be used.
#'
#' @return Data frame with columns id, area, flow, hsorig, hsext, fcl.
#'   id holds row numbers of original dataset. hsorig is input hs. hsext is
#'   input hs with additional zeros if requires. If there are multiple
#'   HS->FCL matchings, all of them are returned with similar id. If
#'   there were no matching FCL codes, NA in fcl column is returned.
#'
#' @details Input hs, areacode and flowname columns are used to build
#'   data frame with hs-codes to convert. Probably it is easier to
#'   pass a data frame, then separate vectors.
#'
#' @import dplyr
#' @export


hsInRange <- function(uniqhs,
                      maptable,
                      parallel = FALSE) {

  df <- uniqhs %>%
    mutate(datumid = row_number())

  # Splitting of trade dataset by area and flow
  # and applying mapping function to each part
  df_fcl <- plyr::ddply(
    df,
    .variables = c("reporter", "flow"),
    .fun = function(subdf) {

      # Subsetting mapping file
      maptable <- maptable %>%
        filter_(~reporter == subdf$reporter[1],
                ~flow == subdf$flow[1])

      # If no corresponding records in map return empty df
      if(nrow(maptable) == 0)
        return(data_frame(
          datumid = subdf$id,
          hs = subdf$hs,
          hsext = subdf$hsext,
          fcl = as.integer(NA),
          maplinkid = as.numeric(NA)))


      # Split original data.frame by row,
      # and looking for matching fcl codes
      # for each input hs code.
      # If there are multiple matchings we return
      # all matches.
      fcl <- plyr::ldply(
        subdf$datumid,
        function(currentid) {

          # Put single hs code into a separate variable
          hsext <- subdf %>%
            filter_(~datumid == currentid) %>%
            select_(~hsext) %>%
            unlist() %>% unname()

          # Original HS to include into output dataset
          hs <- subdf %>%
            filter_(~datumid == currentid) %>%
            select_(~hs) %>%
            unlist() %>% unname()

          maptable <- maptable %>%
            filter_(~fromcodeext <= hsext &
                      tocodeext >= hsext)

          # If no corresponding HS range is
          # available return empty integer
          if(nrow(maptable) == 0L) {
            fcl <- as.integer(NA)
            maplinkid <- as.numeric(NA)
          }

          if(nrow(maptable) >= 1L) {
             fcl <- maptable$fcl
             maplinkid <- maptable$maplinkid
          }

          data_frame(datumid = currentid,
                     hs = hs,
                     hsext = hsext,
                     fcl = fcl,
                     maplinkid = maplinkid)
        }
      )

    },
    .parallel = parallel,
    .progress = ifelse(interactive() &
                         !parallel &
                         # Don't show progress for quick calculations
                         nrow(uniqhs) > 10^4,
                       "text", "none")
  )
}
