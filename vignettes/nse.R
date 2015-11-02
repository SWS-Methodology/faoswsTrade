select <- function(df, vars) {
  vars <- substitute(vars)
  var_pos <- setNames(as.list(seq_along(df)), names(df))
  pos <- eval(vars, var_pos)
  df[, pos, drop = FALSE]
}


tl <- data_frame(reporter = 1:10, fcl = 21:30, value = rnorm(10))
rules <- data_frame(reporter = c(NA, 6, 7),
                    fcl = c(21, 26, NA),
                    value = c(1, .001, 10))

within(tf, {quan[fcl == 10] <- quan * 100})

plyr::alply(rules, 1, function(x) {
  vars <- names(x)
  todrop <- is.na(x)

#   paste0(vars[!todrop], unname(unlist(x[!todrop])),
#          collapse= " == ")
  # lapply(vars[!todrop], function(x) x)
})





z[eval(parse(text = "x == 5 & y == 6"), envir=z),]
parse(text = "x == 5")

do.call(what = `==`, args = list(x, 5), quote = T , envir = as.environment(z))

is.call(parse(text = "x == 5 & y == 6"))
eval(parse(text = "x == 5 & y == 6"))


plyr::alply(adjustments[1:5,], 1, function(r) {
  conditions <- c("year", "flow", "hs", "fcl", "partner") # Add reporter!!!
  action <- c("quantity", "quantity.other", "value", "special")
  conditions <- r[, conditions]
  conditions <- conditions[,!is.na(conditions)]
  # eval(call("&", call("==", quote(reporter), 1), call("==", quote(fcl), 21)), envir = tl)
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
    if(i > 2L) joinedconds <- call("&", joinedconds, listofconds[[i]])
  }

  joinedconds

}) %>% unlist(use.names = FALSE) -> x

