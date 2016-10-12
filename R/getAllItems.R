#' Retrieves data on HS-codes and their descriptions from SWS
#' 
#' @return Data.table with code and description
#' @import dplyr


getAllItems <- function(dmn   = "trade",
                        dtset = "ct_raw_tf",
                        dimension = "measuredItemHS") {
  
  if(!is.SWSEnvir()) stop("No SWS environment detected.")
  
  faosws::GetCodeList(dmn, dtset, dimension) %>% 
    select_(~code, ~description)
  
}
