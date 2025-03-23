#' Converts measuredElementTrade for the output
#'
#' @param data Data.
#'
#' @export

convertMeasuredElementTrade <- function(data) {

  # All cases
  ## Elements:
  ##    - Imports (number): 5607
  ##    - Imports (heads): 5608
  ##    - Imports (1000 heads): 5609
  ##    - Imports (t): 5610
  ##    - Exports (number): 5907
  ##    - Exports (heads): 5908
  ##    - Exports (1000 heads): 5909
  ##    - Exports (t): 5910
  ##    - Imports (kUS$): 5622
  ##    - Exports (kUS$): 5922
  ##    - Imports (US$): 5621
  ##    - Exports (US$): 5921

  # QTY

  data[measuredElementTrade == "qty" & unit == "mt"         & flow == 1, measuredElementTrade_code := "5610"]
  data[measuredElementTrade == "qty" & unit == "mt"         & flow == 2, measuredElementTrade_code := "5910"]

  data[measuredElementTrade == "qty" & unit == "number"     & flow == 1, measuredElementTrade_code := "5607"]
  data[measuredElementTrade == "qty" & unit == "number"     & flow == 2, measuredElementTrade_code := "5907"]

  data[measuredElementTrade == "qty" & unit == "heads"      & flow == 1, measuredElementTrade_code := "5608"]
  data[measuredElementTrade == "qty" & unit == "heads"      & flow == 2, measuredElementTrade_code := "5908"]

  data[measuredElementTrade == "qty" & unit == "1000 heads" & flow == 1, measuredElementTrade_code := "5609"]
  data[measuredElementTrade == "qty" & unit == "1000 heads" & flow == 2, measuredElementTrade_code := "5909"]

  ## UV

  data[measuredElementTrade == "uv" & unit == "mt"         & flow == 1, measuredElementTrade_code := "5630"]
  data[measuredElementTrade == "uv" & unit == "mt"         & flow == 2, measuredElementTrade_code := "5930"]

  data[measuredElementTrade == "uv" & unit == "number"     & flow == 1, measuredElementTrade_code := "5637"]
  data[measuredElementTrade == "uv" & unit == "number"     & flow == 2, measuredElementTrade_code := "5937"]

  data[measuredElementTrade == "uv" & unit == "heads"      & flow == 1, measuredElementTrade_code := "5638"]
  data[measuredElementTrade == "uv" & unit == "heads"      & flow == 2, measuredElementTrade_code := "5938"]

  data[measuredElementTrade == "uv" & unit == "1000 heads" & flow == 1, measuredElementTrade_code := "5639"]
  data[measuredElementTrade == "uv" & unit == "1000 heads" & flow == 2, measuredElementTrade_code := "5939"]

  # Value

  data[measuredElementTrade == "value" & flow == 1, measuredElementTrade_code := "5622"]
  data[measuredElementTrade == "value" & flow == 2, measuredElementTrade_code := "5922"]


  ## Simple solution, whatever not in the case, just to NA
  ## In this way "$ value only" are NAs, and they will be filtered out

  data[is.na(measuredElementTrade_code), measuredElementTrade_code := "999"]

  data[, measuredElementTrade := measuredElementTrade_code]

  data[, measuredElementTrade_code := NULL]

}
