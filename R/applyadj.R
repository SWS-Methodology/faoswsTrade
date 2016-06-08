#' Apply adjustments to trade data
#'
#' @import dplyr
#' @export


applyadj <- function(rep, yr, adjustments, tradedata, dbg = FALSE) {

  adjustments <- adjustments %>%
    filter_(~reporter == rep & (year == yr | is.na(year)))

  adjustments <- adjustments %>%
    filter_(~value != "quantity_other" | is.na(value))

  tradedata <- tradedata %>%
    filter_(~reporter == rep & year == yr)

  if(length(unique(tradedata$year)) > 1)
    stop("More than one year in trade data")

  if(length(unique(tradedata$reporter)) > 1)
    stop("More than one reporter in trade data")

  adjustments <- getlistofadjs(rep, yr, adjustments)

  if(dbg) {
    # if(exists(matching_adjustments)) stop("Variable matching_adjustments already exists")
    matching_adjustments <<- matrix(data = logical(0),
                                    nrow = nrow(tradedata),
                                    ncol = length(adjustments))
  }

  for(i in seq_along((adjustments))) {
    t <- try(tradedata %>%
               # Create logical vector where to apply current adjustment
               mutate_(.dots = adjustments[[i]]$conditions) %>%
               # If condition is NA than not to apply rule
               mutate_(.dots = setNames(list(~ifelse(is.na(applyrule), FALSE, applyrule)), "applyrule")) %>%
               mutate_(.dots = adjustments[[i]]$action)
    )

    if(inherits(t, "try-error")) message(i) else tradedata <- t

    if(dbg) matching_adjustments[,i] <<- tradedata$applyrule
  }

  tradedata


}
