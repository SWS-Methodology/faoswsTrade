#' Subset the HS to FCL map.
#'
#' Remove irrelevant years in the HS to FCL mapping. When there is not
#' a specific year for the mapping, take the nearest year.
#'
#' @param map HS to FCL map.
#' @param year Numeric.
#' @return A tbl which is a subset of \code{map}.
#' @import dplyr
#' @export

hsfclmapSubset <- function(map = NA, year = NA) {

  if (missing(map)) stop('"map" is needed.')

  if (missing(year) | !is.numeric(year)) stop('"year" (numeric) is needed.')

  map %>%
    # Filter out all records from future years
    filter_(~mdbyear <= year) %>%
    # Distance from year of interest to year in the map
    mutate_(yeardistance = ~year - mdbyear) %>%
    # Select nearest year for every reporter
    # if year == 2011 and mdbyear == 2011, then distance is 0
    # if year == 2011 and mdbyear == 2010, distance is 1
    group_by_(~area) %>%
    filter_(~yeardistance == min(yeardistance)) %>%
    ungroup() %>%
    select_(~-yeardistance) %>%
    ## and add trailing 9 to tocode, where it is shorter
    ## TODO: check how many such cases and, if possible, move to manualCorrectoins
    mutate_(tocode = ~faoswsTrade::trailingDigits(fromcode,
                                                 tocode,
                                                 digit = 9))

}
