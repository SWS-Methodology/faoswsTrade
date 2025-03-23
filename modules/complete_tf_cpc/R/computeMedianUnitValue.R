#' Compute the median unit value of the transaction.
#'
#' The median unit value is calculated in a specific-to-generic fashion:
#' 1) by HS-reporter; 2) by HS8-reporter; by HS6-reporter; 3) by HS;
#' 4) by FCL; 5) by flow.
#' The first available of these options will be used (for HS, HS8 and HS6
#' only when there are at least 10 flows).
#'
#' @param tradedata Trade data.
#' @param name A string specifying the name of the variable name that
#'   will contain the median unit value ("uvm" by default).
#' @param keepvars Logical value indicating whether other median unit values
#'   and the number of reporters are reported. By default it is TRUE.
#'
#' @return \code{tradedata} with median unit value. If \code{other} == TRUE
#'   intermediate median unit values and the number of partners on which the
#'   medians are computed are returned.
#'
#' @import dplyr
#' @import data.table
#' @export

computeMedianUnitValue <- function(tradedata = NA, name = 'uvm', keepvars = TRUE) {

  if (missing(tradedata)) stop('"tradedata" is missing.')

  setDT(tradedata)

  tradedata[,
    `:=`(
      hs8 = stringr::str_pad(stringr::str_sub(hs, 1, 8), 8, 'left', '0'),
      hs6 = stringr::str_pad(stringr::str_sub(hs, 1, 6), 6, 'left', '0')
    )
  ][,
    # median UV reporter/HS (full length)
    `:=`(
      n_rep_hs   = .N,
      uvm_rep_hs = median(uv, na.rm = TRUE)
    ),
    .(year, reporter, flow, hs)
  ][,
    # median UV reporter/HS8
    `:=`(
      n_rep_hs8   = .N,
      uvm_rep_hs8 = median(uv, na.rm = TRUE)
    ),
    .(year, reporter, flow, hs8)
  ][,
    # median UV reporter/HS6
    `:=`(
      n_rep_hs6   = .N,
      uvm_rep_hs6 = median(uv, na.rm = TRUE)
    ),
    .(year, reporter, flow, hs6)
  ][,
    # median UV reporter/FCL
    `:=`(
      n_rep_fcl   = .N,
      uvm_rep_fcl = median(uv, na.rm = TRUE)
    ),
    .(year, reporter, flow, fcl)
  ][,
    # median UV HS (full length)
    `:=`(
      n_hs   = .N,
      uvm_hs = median(uv, na.rm = TRUE)
    ),
    .(year, flow, hs)
  ][,
    # median UV HS8
    `:=`(
      n_hs8   = .N,
      uvm_hs8 = median(uv, na.rm = TRUE)
    ),
    .(year, flow, hs8)
  ][,
    # median UV HS6
    `:=`(
      n_hs6   = .N,
      uvm_hs6 = median(uv, na.rm = TRUE)
    ),
    .(year, flow, hs6)
  ][,
    # median UV FCL
    `:=`(
      n_fcl   = .N,
      uvm_fcl = median(uv, na.rm = TRUE)
    ),
    .(year, flow, fcl)
  ][,
    # median UV reporter/flow
    `:=`(
      uvm_rep_flow = median(uv, na.rm = TRUE)
    ),
    .(year, reporter, flow)
  ]

  tradedata <- tbl_df(tradedata)

  tradedata[[name]] <- tradedata %>% 
    {case_when(
      !is.na(.$uvm_rep_hs)  & .$n_rep_hs  > 10 ~ .$uvm_rep_hs,
      !is.na(.$uvm_rep_hs8) & .$n_rep_hs8 > 10 ~ .$uvm_rep_hs8,
      !is.na(.$uvm_rep_hs6) & .$n_rep_hs6 > 10 ~ .$uvm_rep_hs6,
      !is.na(.$uvm_rep_fcl) & .$n_rep_fcl > 10 ~ .$uvm_rep_fcl,
      !is.na(.$uvm_hs)                         ~ .$uvm_hs,
      !is.na(.$uvm_hs8)                        ~ .$uvm_hs8,
      !is.na(.$uvm_hs6)                        ~ .$uvm_hs6,
      !is.na(.$uvm_fcl)                        ~ .$uvm_fcl,
      !is.na(.$uvm_rep_flow)                   ~ .$uvm_rep_flow
    )}

  if (keepvars == FALSE) {
    tradedata <- tradedata %>%
      dplyr::select(-starts_with('n_rep'), -starts_with('uvm_'), -n_hs)
  }

  return(tbl_df(tradedata))
}
