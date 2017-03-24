#' Compute the median unit value of the transaction.
#'
#' The median unit value is calculated in a specific-to-generic fashion:
#' 1) by HS-reporter; 2) by HS8-reporter; by HS6-reporter; 3) by HS;
#' 4) by FCL; 5) by flow.
#' The first available of these options will be used (for HS, HS8 and HS6
#' only when there are at least 10 flows).
#'
#' @param tradedata Trade data.
#' @param output A string specifying the name of the variable name that
#'   will contain the median unit value ("uvm" by default).
#' @param other Logical value indicating whether other median unit values
#'   and the number of reporters are reported. By default it is TRUE.
#' @return \code{tradedata} with median unit value. If \code{other} == TRUE
#'   intermediate median unit values and the number of partners on which the
#'   medians are computed are returned.
#' @import dplyr
#' @export

computeMedianUnitValue <- function(tradedata = NA, name = 'uvm', other = TRUE) {

  if (missing(tradedata)) stop('"tradedata" is missing.')

  tradedata <- tradedata %>%
    mutate(
           hs8 = stringr::str_sub(hs, 1, 8),
           hs6 = stringr::str_sub(hs, 1, 6)
           ) %>%
    mutate(
           hs8 = if_else(nchar(hs8) == 7, paste0('0', hs8), hs8),
           hs6 = if_else(nchar(hs6) == 5, paste0('0', hs6), hs6)
           ) %>%
    # median value reporter/HS (full length)
    group_by_(~year, ~reporter, ~flow, ~hs) %>%
    mutate_(n_rep_hs = ~n(), uvm_rep_hs = ~median(uv, na.rm = TRUE)) %>%
    # median value reporter/HS8
    group_by_(~year, ~reporter, ~flow, ~hs8) %>%
    mutate_(n_rep_hs8 = ~n(), uvm_rep_hs8 = ~median(uv, na.rm = TRUE)) %>%
    # median value reporter/HS6
    group_by_(~year, ~reporter, ~flow, ~hs6) %>%
    mutate_(n_rep_hs6 = ~n(), uvm_rep_hs6 = ~median(uv, na.rm = TRUE)) %>%
    # median value HS
    group_by_(~year, ~flow, ~hs) %>%
    mutate_(n_hs = ~n(), uvm_hs = ~median(uv, na.rm = TRUE)) %>%
    # median value FCL
    group_by_(~year, ~flow, ~fcl) %>%
    mutate_(uvm_fcl = ~median(uv, na.rm = TRUE)) %>%
    # median value flow
    group_by_(~year, ~flow) %>%
    mutate_(uvm_flow = ~median(uv, na.rm = TRUE)) %>%
    ungroup()

  tradedata[[name]] <- tradedata %>% 
    {case_when(
      !is.na(.$uvm_rep_hs)  & .$n_rep_hs  > 10 ~ .$uvm_rep_hs,
      !is.na(.$uvm_rep_hs8) & .$n_rep_hs8 > 10 ~ .$uvm_rep_hs8,
      !is.na(.$uvm_rep_hs6) & .$n_rep_hs6 > 10 ~ .$uvm_rep_hs6,
      !is.na(.$uvm_hs)                         ~ .$uvm_hs,
      !is.na(.$uvm_fcl)                        ~ .$uvm_fcl,
      # XXX
      !is.na(.$uvm_flow)                       ~ .$uvm_flow
    )}

  if (other == FALSE) {
    tradedata <- tradedata %>%
      select(-starts_with('n_rep'), -starts_with('uvm_'), -n_hs)
  }

  return(tradedata)
}
