#' Melt trade database
#'
#' @param data data from SWS.
#'
#' @return Melted data.
#'
#' @import dplyr
#'
#' @export

meltTradeData <- function(data = NA) {
  reshape2::melt(data,
                 id=c('geographicAreaM49Reporter',
                      'geographicAreaM49Partner',
                      'measuredElementTrade',
                      'measuredItemCPC',
                      'timePointYears',
                      'flagObservationStatus',
                      'flagMethod'),
                 value.name = 'Value') %>%
    tbl_df() %>%
    select(-variable, -L1)
}

