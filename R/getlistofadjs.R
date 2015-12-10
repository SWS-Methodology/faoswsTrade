#' Generate list of adjustments from a table extracted from MDB notes
#'
#' @import dplyr

getlistofadjs <- function(rep, yr, adjustments) {

  # Probably filter is not needed (already done in applyadj.R before calling getlistofadjs)
  # BUT distinct YES!!!
  adjustments <- adjustments %>%
    filter_(~reporter == rep & (year == yr | is.na(year))) %>%
    distinct()

  plyr::alply(adjustments, 1, function(r) {
    conditions <- c("flow", "hs", "fcl", "partner")
    action <- c("weight", "qty", "value", "special")

    ### Conditions

    conditions <- r[, conditions]
    # Drop columns which are not part of the condition
    conditions <- conditions[,!is.na(conditions), drop = F]

    # If rule applies to all flows there are no conditions
    if(ncol(conditions) > 0) {
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
    } else {
      # If rule applies to all rows (no conditions)
      joinedconds <- list(TRUE)
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

    # Action 1. Multiply column itself by coeff (no special)
    if(stringr::str_detect(one, "^\\d*\\.?\\d*$") & nospecial) {
      one <- as.numeric(one)

      action <- lazyeval::interp(as.call(list(`*`, as.name(target), one)),
                                 target = target,
                                 one = one)
    }

    # Action 2. Multiply column by special
    if(stringr::str_detect(one, "^value$|^weight$|^qty$") & !nospecial) {

      action <- lazyeval::interp(as.call(list(`*`, as.name(one), special)),
                                 one = one,
                                 special = special)
    }

    # Action 3. SET value/quantity to a constant
    if(stringr::str_detect(one, "^[m,f,o]\\d*\\.?\\d*$")) {
      one <- stringr::str_replace(one, "^[m,f,o]", "")
      one <- as.numeric(one)

      action <- one

    }

    # Action 4. SET value/quantity to value from other column
    if(stringr::str_detect(one, "^value$|^weight$|^qty$") & nospecial) {

      action <- as.name(one)
    }


    # General procedures for all types of actions

    # ifelse() in case rule is not applied than we return current value
    # P.S.: It is better to move ifelse inside of apply part, to make list more clear
    action <- as.call(list(ifelse, quote(applyrule),
                           action,
                           as.name(target)))

    action <- setNames(list(action), target)


    list(conditions = joinedconds,
         action = action)


  },
  .progress = "none",
  .inform = FALSE,
  .parallel = FALSE)


}
