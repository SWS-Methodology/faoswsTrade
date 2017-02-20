#' Select one record from multiple matches during HS->FCL mapping process.
#'
#' @param mapsubset Data frame with columns reporter, flow, hs, fcl
#'
#' @export
#' @import dplyr
#'

sel1FCL <- function(hsfclmatch, maptable) {
  
  if(length(unique(hsfclmatch$datumid)) > 1L) return(
    hsfclmatch %>% 
      group_by_(~datumid) %>% 
      do(sel1FCL(., maptable)) %>% 
      ungroup
  )
  
  if(nrow(hsfclmatch) == 1L) return(hsfclmatch)
  
  # Selection of the narrowest hs range
  hsfclmatch %>% 
    left_join(maptable, by = "maplinkid") %>% 
    mutate_(hsrange = ~tocodeext - fromcodeext) %>%
    filter_(~hsrange == min(hsrange))
  
  # If there are still several options
  # we choose the yougest
  if(nrow(hsfclmatch) > 1L) {
    hsfclmatch <- hsfclmatch %>%
      filter_(~maplinkid == max(maplinkid))
  }
  
  stopifnot(nrow(hsfclmatch) == 1L)
  
  hsfclmatch %>% 
    select_(~reporter, ~flow, ~datumid, ~hs, ~hsext, ~fcl, ~maplinkid)
  
}
