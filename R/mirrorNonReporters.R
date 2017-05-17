#' Mirror non-reporting and incomplete countries.
#'
#' Use mirror trade flows for imputing trade data for non-reporting
#' countries and for countries that do not report a flow.
#'
#' For non-reporting and incomplete countries exports of reporters will
#' become their imports, and vice versa. In order to take into account
#' the different international shipping costs, a CIF (Cost, Insurance,
#' Freight) / FOB (Free On Board) correction is done. Currently, the
#' CIF/FOB correction is set at 12% (imports are increased by 12% the
#' values of reporters' exports).
#'
#' @param tradedata Trade data.
#' @param mirror data.frame containing the code of the area, the
#'   flow and the number of flows.
#' @return \code{tradedata} with mirrored data for non-reporters added.
#' @import dplyr
#' @export

mirrorNonReporters <- function(tradedata = NA, mirror = NA) {

  if (missing(tradedata)) stop('"tradedata" is missing.')

  if (missing(mirror)) stop('"mirror" is missing.')

  rprt_writetable(mirror, 'countries_to')

  tradedatanonrep <- tradedata %>%
    left_join(
      mirror %>% rename_(partner = ~area) %>% mutate(i = 1),
      by = c("partner", "flow")
    ) %>%
    filter_(~i == 1) %>%
    select_(~-i) %>%
    mutate_(
      partner_mirr = ~reporter,
      partner_mirrM49 = ~reporterM49,
      reporter = ~partner,
      reporterM49 = ~partnerM49,
      partner = ~partner_mirr,
      partnerM49 = ~partner_mirrM49,
      flow = ~recode(flow, '2' = 1, '1' = 2),
      ## CIF/FOB correction (fixed at 12%; further analyses needed)
      value = ~ifelse(flow == 1, value*1.12, value/1.12)
    ) %>%
    select_(~-partner_mirr, ~-partner_mirrM49)

  bind_rows(tradedata, tradedatanonrep)
}
