#' Select one record from multiple matches during HS->FCL mapping process.
#'
#' @param mapsubset Data frame with columns reporter, flow, hs, fcl
#'
#' @export
#'

sel1FCL <- function(mapsubset) {

             # Selection of the narrowest hs range
            mapsubset <- mapsubset %>%
              mutate_(hsrange = ~tocode - fromcode) %>%
              filter_(~hsrange == min(hsrange))

            # If there are still several options
            # we choose the yougest
            if(nrow(mapsubset) > 1L) {
              mapsubset <- mapsubset %>%
               filter_(~recordnumb == max(recordnumb))
            }

            stopifnot(nrow(mapdataset) == 1L)


}
