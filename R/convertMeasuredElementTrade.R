#' Converts measuredElementTrade for the output
#'
#' @param element Element.
#' @param unit Unit.
#' @param flow Flow.
#'
#' @export

convertMeasuredElementTrade <- function(element, unit, flow) {

  # All cases
  ## Elements:
  ##    - Imports (heads): 5608
  ##    - Imports (1000 heads): 5609
  ##    - Imports (t): 5610
  ##    - Exports (heads): 5908
  ##    - Exports (1000 heads): 5909
  ##    - Exports (t): 5910
  ##    - Imports (kUS$): 5622
  ##    - Exports (kUS$): 5922
  ##    - Imports (US$): 5621
  ##    - Exports (US$): 5921

  element <- as.character(element)
  unit <- as.character(unit)
  flow <- as.numeric(flow)

  newElement <-
           if((element == "qty") & (unit == "mt") & (flow == 1)) {
      "5610"
    } else if((element == "qty") & (unit == "mt") & (flow == 2)) {
      "5910"
    } else if((element == "qty") & (unit == "heads") & (flow == 1)) {
      "5608"
    } else if((element == "qty") & (unit == "heads") & (flow == 2)) {
      "5908"
    } else if((element == "qty") & (unit == "1000 heads") & (flow == 1)) {
      "5609"
    } else if((element == "qty") & (unit == "1000 heads") & (flow == 2)) {
      "5909"
      ## begin add unit values
    } else if((element == "uv") & (unit == "mt") & (flow == 1)) {
      "5630"
    } else if((element == "uv") & (unit == "mt") & (flow == 2)) {
      "5930"
    } else if((element == "uv") & (unit == "heads") & (flow == 1)) {
      "5638"
    } else if((element == "uv") & (unit == "heads") & (flow == 2)) {
      "5938"
    } else if((element == "uv") & (unit == "1000 heads") & (flow == 1)) {
      "5639"
    } else if((element == "uv") & (unit == "1000 heads") & (flow == 2)) {
      "5939"
      ## end add unit values
    } else if((element == "value") & (flow == 1)) {
      "5622"
    } else if((element == "value") & (flow == 2)) {
      "5922"
    } else {"999"}
  ## Simple solution, whatever not in the case, just to NA
  ## In this way "$ value only" are NAs, and they will be filtered out

  newElement

}
