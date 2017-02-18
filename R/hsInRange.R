#' Looks for corresponding FCL codes in country-specific
#' mapping tables from MDB files
#'
#' @param hs Numeric or character vector with HS codes to convert to FCL.
#' @param areacode Numeric or character vector with reporters' codes.
#' @param flowname Numeric or character vector of trade direction.
#' @param mapdataset Data frame with HS->FCL mapping with columns area,
#'   flow, fromcode, tocode, fcl
#'   registered with doParallel package.
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
                      mapdataset,
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
      mapdataset <- mapdataset %>%
        filter_(~reporter == subdf$reporter[1],
                ~flow == subdf$flow[1])

      # If no corresponding records in map return empty df
      if(nrow(mapdataset) == 0)
        return(data_frame(
          datumid = subdf$id,
          hs = subdf$hs,
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
          hs <- subdf %>%
            filter_(~id == currentid) %>%
            select_(~hs) %>%
            unlist() %>% unname()

          mapdataset <- mapdataset %>%
            filter_(~fromcodeext <= hs &
                      tocodeext >= hs)

          # If no corresponding HS range is
          # available return empty integer
          if(nrow(mapdataset) == 0L) {
            fcl <- as.integer(NA)
            maplinkid <- as.numeric(NA)
          }

          if(nrow(mapdataset) >= 1L) {
             fcl <- mapdataset$fcl
             maplinkid <- mapdataset$maplinkid
          }

          data_frame(id = currentid,
                     hs = hs,
                     fcl = fcl,
                     maplinkid = maplinkid)
        }
      )

    },
    .parallel = parallel,
    .progress = ifelse(interactive() &
                         !parallel &
                         nrow(uniqhs) > 10^4,
                       "text", "none")
  )
}
