#' Remove invalid reporters.
#'
#' @param tradedata TL or ES trade data.
#' @return TL or ES data without invalid reporters.
#' @import dplyr
#' @export

removeInvalidReporters <- function(tradedata) {

  if (missing(tradedata)) stop('"tradedata" should be set.')

  stopifnot("reporter" %in% colnames(tradedata))

  # Create a table with valid reporters
  valid_reporters <-
    GetCodeList(
      domain    = 'faostat_one',
      dataset   = 'FS1_SUA_UPD',
      dimension = 'geographicAreaFS'
    ) %>%
    filter(type != 'group') %>%
    mutate(
      startDate = as.numeric(stringr::str_sub(startDate, 1, 4)),
      endDate   = as.numeric(stringr::str_sub(endDate, 1, 4))
    ) %>%
    select(-selectionOnly, -type) %>%
    filter(startDate <= year, endDate >= year)

  reporters_to_drop <- setdiff(unique(tradedata$reporter), valid_reporters$code)

  if (length(reporters_to_drop) > 0) {
    flog.trace("TL: dropping invalid reporters", name = "dev")
    x <- ifelse(length(reporters_to_drop) == 1, 'y', 'ies')
    warning(paste0('The following countr', x, ' did not exist in ', year, ': ',
                  paste(reporters_to_drop, collapse=' ')))

    # Keep valid reporters
    tradedata %>%
      filter(reporter %in% valid_reporters$code)
  } else {
    tradedata
  }

}
