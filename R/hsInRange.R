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


hsInRange <- function(hs,
                      areacode,
                      flowname,
                      mapdataset,
                      parallel = FALSE) {

  # Constructing of data.frame with original hs codes.
  if(!all.equal(length(hs),
                length(areacode),
                length(flowname)))
    stop("Vectors of different length")

  stopifnot("recordnumb" %in% colnames(mapdataset))

  df <- data_frame(hs = hs,
                   areacode = areacode,
                   flowname = flowname) %>%
    distinct %>%
    mutate(id = row_number())



  # Splitting of trade dataset by area and flow
  # and applying mapping function to each part
  # TODO: define the function outside and refer to
  # it by name for easy performance profiling
  df_fcl <- plyr::ddply(
    df,
    .variables = c("areacode", "flowname"),
    .fun = function(subdf) {

      # Subsetting mapping file
      mapdataset <- mapdataset %>%
        filter_(~area == subdf$areacode[1],
                ~flow == subdf$flowname[1])

      # If no corresponding records in map return empty df
      if(nrow(mapdataset) == 0)
        return(data_frame(
          id = subdf$id,
          hs = subdf$hs,
          fcl = as.integer(NA),
          recordnumb = as.numeric(NA)))

      # Align length of HS codes in map and dataset
      # to make possible comparison of numbers
      aligned <- alignHSLength(subdf$hs, mapdataset)

      #Replacing original hs codes by aligned versions
      subdf$hs <- aligned$hs
      mapdataset <- aligned$mapdataset

      # Split original data.frame by row,
      # and looking for matching fcl codes
      # for each input hs code.
      # If there are multiple matchings we return
      # all matches.
      fcl <- plyr::ldply(
        subdf$id,
        function(currentid) {

          # Put single hs code into a separate variable
          hs <- subdf %>%
            filter_(~id == currentid) %>%
            select_(~hs) %>%
            unlist() %>% unname()

          mapdataset <- mapdataset %>%
            filter_(~fromcode <= hs &
                      tocode >= hs)

          # If no corresponding HS range is
          # available return empty integer
          if(nrow(mapdataset) == 0) {
            fcl <- as.integer(NA)
            recordnumb <- as.numeric(NA)
          }

          if(nrow(mapdataset) == 1L) {
             fcl <- mapdataset$fcl
             recordnumb <- mapdataset$recordnumb
          }
             # Selection of the narrowest hs range
          if(nrow(mapdataset) > 1L) {
            mapdataset <- mapdataset %>%
              mutate_(hsrange = ~tocode - fromcode) %>%
              filter_(~hsrange == min(hsrange))

            # If there are still several options
            # we choose the yougest
            if(nrow(mapdataset) > 1L) {
              mapdataset <- mapdataset %>%
               filter_(~recordnumb == max(recordnumb))
            }

            stopifnot(nrow(mapdataset) == 1L)

            fcl <- mapdataset$fcl
            recordnumb <- mapdataset$recordnumb
          }

          data_frame(id = currentid,
                     hs = hs,
                     fcl = fcl,
                     recordnumb = recordnumb)
        }
      )

    },
    .parallel = parallel,
    .progress = ifelse(interactive() & !parallel, "text", "none")
  )

  df <- df %>%
    rename_(hsorig = ~hs) %>%
    # Joining original trade dataset
    # with new dataset containing fcl codes
    left_join(df_fcl,
              by = c("id", "areacode", "flowname")) %>%
    select_(~id,
            reporter = ~areacode,
            flow = ~flowname,
            hs = ~hsorig,
            hsext = ~hs,
            ~fcl,
            ~recordnumb)

  df
}
