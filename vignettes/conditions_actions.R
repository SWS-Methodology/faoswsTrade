
#
# ustrade <- tradedata %>%   filter(reporter == 231)
#
# rep <- 231

getlistofadjs <- function(rep, yr, adjustments) {

  adjustments <- adjustments %>%
    filter_(~reporter == rep & (year == yr | is.na(year)))

  plyr::alply(adjustments, 1, function(r) {
    conditions <- c("flow", "hs", "fcl", "partner")
    action <- c("weight", "qty", "value", "special")

    ### Conditions

    conditions <- r[, conditions]
    # Drop columns which are not part of the condition
    conditions <- conditions[,!is.na(conditions), drop = F]

    conditions <- data.frame(var = names(conditions),
                             value = unname(unlist(conditions)),
                             stringsAsFactors = F)
    listofconds <- unlist(plyr::alply(conditions, 1, function(x) {
      call("==", as.name(x$var), x$value)
    }),
    use.names = FALSE)

    for(i in seq_along(listofconds)) {
      if(i == 1L) joinedconds <- listofconds
      if(i == 2L) joinedconds <- call("&", listofconds[[1L]], listofconds[[2L]])
      if(i > 2L)  joinedconds <- call("&", joinedconds, listofconds[[i]])
    }

    # Without list we can't specify name in mutate_
    if(!is.list(joinedconds)) joinedconds <- list(joinedconds)
    joinedconds <- setNames(joinedconds, "applyrule")

    # joinedconds <- unlist(joinedconds, use.names = FALSE)

    ### Actions

    action <- r[, action]
    # Drop columns which are not part of the action
    action <- action[,!is.na(action), drop = F]

    if(ncol(action) > 2L) stop(
      paste0("More than one target in action"))

    target <- colnames(action)[colnames(action) != "special"]

    if(length(target) > 1L) stop("Two targets and no special")

    nospecial <- !is.element("special", colnames(action))

    if(!nospecial) special <- as.numeric(action[1, "special", drop = T])

    # Value from target column
    one <- action[1, target, drop = T]

    # Multiply column itself by coeff (no special)
    if(stringr::str_detect(one, "^\\d*\\.?\\d*$") & nospecial) {
      one <- as.numeric(one)

      action <- lazyeval::interp(as.call(list(`*`, as.name(target), one)),
                                 target = target,
                                 one = one)

      # It is better to move ifelse inside of apply part, to make list more clear
      action <- as.call(list(ifelse, quote(applyrule), action, as.name(target)))

    }

    # Multiply column by special
    if(stringr::str_detect(one, "^value$|^weight$|^qty$") & !nospecial ) {
#       action <- lazyeval::interp(~ifelse(applyrule, one * special, target),
#                                  one = as.name(one),
#                                  target = as.name(target),
#                                  special = special)

      action <- lazyeval::interp(as.call(list(`*`, as.name(one), special)),
                                 one = one,
                                 special = special)
    }

    action <- setNames(list(action), target)


    list(conditions = joinedconds,
         action = action)


  },
  .progress = "text",
  .inform = FALSE,
  .parallel = FALSE)


}


applyadj <- function(rep, yr, adjustments, tradedata) {

  adjustments <- adjustments %>%
    filter_(~reporter == rep & (year == yr | is.na(year)))

  tradedata <- tradedata %>%
    filter_(~reporter == rep & year == yr)

  if(length(unique(tradedata$year)) > 1)
    stop("More than one year in trade data")

  if(length(unique(tradedata$reporter)) > 1)
    stop("More than one reporter in trade data")

  adjustments <- getlistofadjs(rep, yr, adjustments)

  for(i in seq_along((adjustments))) {


    t <- try(tradedata %>%
      # Create logical vector where to apply current adjustment
      mutate_(.dots = adjustments[[i]]$conditions) %>%
      mutate_(.dots = setNames(list(~ifelse(is.na(applyrule), FALSE, applyrule)), "applyrule")) %>%
      mutate_(.dots = adjustments[[i]]$action)
    )

    if(inherits(t, "try-error")) message(i) else tradedata <- t
  }

  tradedata


}
#
# ustrade %>%
#   mutate_(.dots = ifelse(~value > 15, usadj[[137]]$action) %>% head
