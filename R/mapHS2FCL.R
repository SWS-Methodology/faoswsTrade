#' Function to map HS codes to FCL codes
#'
#' @import dplyr
#' @export

mapHS2FCL <- function(tradedata, maptable, parallel = FALSE) {

  # Extract unique input combinations ####

  uniqhs <- tradedata %>%
    select_(~reporter, ~flow, ~hs) %>%
    distinct

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
            tocodeext   = ~as.numeric(tocodeextchar),
            # Is it legitimate to create row numbers in such way?
            maplinkid   = ~row_number(reporter)) 


  # Find mappings ####
  uniqhs <- hsInRange(uniqhs, maptable, parallel = parallel) 

  # Choose ones from multiple matches ####

  uniqhs <- sel1FCL(uniqhs, maptable)
  
  # Join original trade dataset with mapping ####
  
 tradedata %>% 
    left_join(uniqhs, by = c("reporter", "flow", "hs"))

}
