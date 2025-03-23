#' Get HS codes with many digits.
#'
#'
#' This function selects the HS codes that have many lengths.
#'
#' @param data Trade data including both TL and ES.
#' @import data.table
#' @export

getHsManyLengths <- function(data) {

  data = data.table(data)
  data[, ncharac := nchar(hs)]
  data[, hs6 := substr(hs, 1, 6)]
  dataAggregatedHS6 = data[, list(totValue = sum(value, na.rm = T)),
                           by = list(year, reporter, partner, flow,
                                     hs6, fclunit, ncharac)]

  tradedataaggregatedHS6 = dcast.data.table(dataAggregatedHS6,
                                            year + reporter + partner +
                                              flow + hs6 + fclunit ~ ncharac,
                                            value.var = "totValue")


  # HT Sebastian
  tradedataaggregatedHS6_num_cols <- tradedataaggregatedHS6[, mget(grep("^[[:digit:]]{1,2}$", names(tradedataaggregatedHS6), value = TRUE))]

  tradedatareportPartManyHs = tradedataaggregatedHS6[apply(tradedataaggregatedHS6_num_cols, 1, function(x)  sum(!is.na(x))) > 1, ]

  tradedatareportPartManyHs[, geographicAreaM49Reporter := fs2m49(as.character(reporter))]
  tradedatareportPartManyHs[, geographicAreaM49Partner := fs2m49(as.character(partner))]

  tradedatareportPartManyHs = nameData("trade", "completed_tf_m49", tradedatareportPartManyHs)

  setnames(tradedatareportPartManyHs, "geographicAreaM49Reporter_description", "reporter_name")
  setnames(tradedatareportPartManyHs, "geographicAreaM49Partner_description", "partner_name")
  setnames(tradedatareportPartManyHs, "geographicAreaM49Reporter", "reporterM49")
  setnames(tradedatareportPartManyHs, "geographicAreaM49Partner", "partnerM49")
  tradedatareportPartManyHs[, c("reporter", "partner") := NULL]
  setcolorder(tradedatareportPartManyHs, c("year", "reporter_name", "reporterM49", "partner_name", "partnerM49", "flow", "hs6", "fclunit", "6", "7","8", "9","10","11"))
  colnames(tradedatareportPartManyHs)[-c(1:8)] = paste(
    "digits", colnames(tradedatareportPartManyHs[, -c(1:8)]),
    sep = "_")


  return(tradedatareportPartManyHs)
}
