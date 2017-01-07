#' Use specific adjustment factors for trade data.
#'
#' @param tradedata Trade data.
#' @param year Numeric.
#' @param adjustments Adjustment factors.
#' @param parallel Logical: FALSE (dafault), no multicore; TRUE, multicore.
#' @param dbg Logical: see \code{\link[faosws]{CheckDebug}}.
#' @param verbose Logical: TRUE (default), message shown; FALSE, no message.
#' @param PID Numeric: a process id (can also be a string). Used if
#'   \code{verbose == TRUE}.
#' @return The same dataset after the application of adjustment factors.
#' @import dplyr
#' @export

useAdjustments <- function(tradedata = stop('"tradedata" is missing.'),
                           year = stop('"year" is missing.'),
                           adjustments = stop('"adjustments" is missing.'),
                           parallel = FALSE,
                           dbg = faosws::CheckDebug(),
                           PID = NA,
                           verbose = TRUE) {
  
  if (!is.logical(parallel)) stop('"parallel" should be TRUE or FALSE.')

  if (!is.logical(dbg)) stop('"dbg" should be TRUE or FALSE.')

  adj <- as.data.frame(adjustments)

  prog <- !parallel && dbg

  if (verbose & !missing(PID)) { 
    d <- as.character(substitute(tradedata))
    message(sprintf("[%s] Applying adjustments to %s", PID, d))
  }

  # Needed, because of multiple rows
  tradedata$key <- 1:nrow(tradedata)

  tradedata_adj <- tbl_df(plyr::ldply(sort(unique(tradedata$reporter)),
                               function(x) {
                                 applyadj(x, year, adj, tradedata)
                               },
                               .progress = ifelse(prog, "text", "none"),
                               .inform = FALSE,
                               .parallel = parallel))
  tradedata <- tradedata %>%
                 rename(orig_value=value, orig_weight=weight, orig_qty=qty)

  tradedata_adj %>%
    left_join(tradedata) %>%
    mutate(
           # Using coalesce to replace NAs with -1 as NAs will expand
           adj_value  = !(near(coalesce(value, -1),  coalesce(orig_value, -1))),
           adj_weight = !(near(coalesce(weight, -1), coalesce(orig_weight, -1))),
           adj_qty    = !(near(coalesce(qty, -1),    coalesce(orig_qty, -1))),
           adjusted   = adj_value | adj_weight | adj_qty
           ) %>%
    select(-orig_value, -orig_weight, -orig_quantity)
}
