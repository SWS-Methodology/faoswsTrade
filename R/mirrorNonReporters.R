#' Mirror non-reporting countries.
#'
#' Use mirror trade flows for imputing trade data for non-reporting
#' countries.
#'
#' For non-reporting countries exports of reporters will become their
#' imports, and vice versa. In order to take into account the different
#' international shipping costs, a CIF (Cost, Insurance, Freight) / FOB
#' (Free On Board) correction is done. Currently, the CIF/FOB correction
#' is set at 12% (imports are increased by 12% the values of reporters'
#' exports).
#'
#' @param tradedata Trade data.
#' @param nonreporters Vector containing the codes of non-reporting countries.
#' @return \code{tradedata} with mirrored data for non-reporters added.
#' @import dplyr
#' @export

mirrorNonReporters <- function(tradedata = NA, nonreporters = NA) {

  if (missing(tradedata)) stop('"tradedata" is missing.')

  if (missing(nonreporters)) stop('"nonreporters" is missing.')

  tradedatanonrep <- tradedata %>%
    filter_(~partner %in% nonreporters) %>%
    mutate_(partner_mirr = ~reporter,
            partner_mirrM49 = ~reporterM49,
            reporter = ~partner,
            reporterM49 = ~partnerM49,
            partner = ~partner_mirr,
            partnerM49 = ~partner_mirrM49,
            flow = ~recode(flow, '2' = 1, '1' = 2),
            ## CIF/FOB correction (fixed at 12%; further analyses needed)
            value = ~ifelse(flow == 1,
                            value*1.12,
                            value/1.12)) %>%
    select_(~-partner_mirr, ~-partner_mirrM49)
  
  bind_rows(tradedata, tradedatanonrep)
}
