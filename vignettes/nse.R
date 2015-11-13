

plyr::alply(adjustments %>%
              filter_(~year == 2011 | is.na(year)), 1, function(r) {
  conditions <- c("reporter", "year", "flow", "hs", "fcl", "partner")
  action <- c("quantity", "quantity.other", "value", "special")

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

  joinedconds

  #### Actions



},
.progress = "text",
.inform = FALSE,
.parallel = TRUE) %>% unlist(use.names = FALSE) -> conditions

adjustments %>% select_("quantity", "quantity.other", "value", "special") %>% sample_n(10)
