#' Prepares for numerical comparison vector of hs codes from
#' trade dataset with hs codes range from mapping table.
#'
#' @param uniqhs Data frame with columns `reporter`, `flow` and `hs` containing
#' unique combinations from a trade data set.
#' @param mapdataset Data frame with mapping table containing
#'   at least columns area, flow, fromcode, tocode
#'
#' @return A data frame with columns `reporter`, `flow` and `maxhslength`.
#'
#' @import dplyr
#' @export

maxHSLength <- function(uniqhs, mapdataset, parallel = FALSE) {

  # Vectorizing over reporter and flow in tradedata

  if(length(unique(uniqhs$reporter)) > 1L |
     length(unique(uniqhs$flow)) > 1L) return(
       plyr::ddply(.data = uniqhs,
                   .variables = c("reporter", "flow"),
                   .parallel = parallel,
                   .fun = maxHSLength,
                   .paropts = list(.packages = "dplyr"),
                   mapdataset
       )
     )

  # Filter mapping table by current reporter and flow
  mapdataset <- mapdataset %>%
    filter_(~area == uniqhs$reporter[1],
            ~flow == uniqhs$flow[1])

  maxhslength   <- max(stringr::str_length(uniqhs$hs))
  maxfromlength <- max(stringr::str_length(mapdataset$fromcode))
  maxtolength   <- max(stringr::str_length(mapdataset$tocode))
  uniqhs$maxhslength  <- max(maxhslength, maxtolength, maxfromlength)

  uniqhs %>%
    select_(~reporter, ~flow, ~maxhslength) %>%
    distinct
}
