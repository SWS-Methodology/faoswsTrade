#' Umbrella function to map HS codes to FCL codes
#'
#' The function takes trade data set with mapping table and finds corresponding
#' FCL codes for specific reporter, flow and HS code combinations.
#'
#' @param tradedata Trade data frame. Columns reporter, flow and hs are expected.
#' @param maptable Mapping table.
#' @param parallel Logical, should multicore backend be used. False by default.
#'
#' @return Original trade data frame with additional FCL column.
#'
#' @import dplyr
#' @export

mapHS2FCL <- function(tradedata, maptable, parallel = FALSE) {

  # Name for passing to reporting functions
  tradedataname <- lazyeval::expr_text(tradedata)
  
  # Extract unique input combinations ####

  uniqhs <- tradedata %>%
    select_(~reporter, ~flow, ~hs) %>%
    distinct

  rprt_uniqhs(uniqhs, tradedataname = tradedataname)

  ## Align HS codes from data and table #####

  hslength <- maxHSLength(uniqhs, maptable, parallel = parallel)

  uniqhs <- uniqhs %>%
    left_join(hslength, by = c("reporter", "flow")) %>%
    mutate_(hsextchar = ~stringr::str_pad(hs,
                                          width = maxhslength,
                                          side = "right",
                                          pad = "0"),
            hsext = ~as.numeric(hsextchar))

  maptable <- hslength %>%
    left_join(maptable, by = c("reporter" = "area", "flow")) %>%
    mutate_(fromcodeextchar = ~stringr::str_pad(fromcode,
                                         width = maxhslength,
                                         side = "right",
                                         pad = "0"),
            tocodeextchar = ~stringr::str_pad(tocode,
                                       width = maxhslength,
                                       side = "right",
                                       pad = "9")) %>%
    mutate_(fromcodeext = ~as.numeric(fromcodeextchar),
            tocodeext   = ~as.numeric(tocodeextchar))


  # Find mappings ####
  uniqhs <- hsInRange(uniqhs, maptable, parallel = parallel)

  # Choose ones from multiple matches ####

  uniqhs <- sel1FCL(uniqhs, maptable)

  # Join original trade dataset with mapping ####

 tradedata %>%
    left_join(uniqhs, by = c("reporter", "flow", "hs"))

}
