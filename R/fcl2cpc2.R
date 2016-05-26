fcl2cpc2  <- function(fclCodes){
  warning("This function need to refers to a table in the SWS and not to a local one.")
  stopifnot(is(fclCodes, "character"))
  data("fcl_2_cpc2", package = "faoswsTrade", envir = environment())
  #if (!exists("swsContext.datasets"))
  #  stop("No swsContext.datasets object defined.  Thus, you probably ",
  #       "won't be able to read from the SWS and so this function won't ",
  #       "work.")
  codeLength = sapply(fclCodes, nchar)
  if (any(codeLength != 4))
    stop("All FCL codes must be 4 characters long!  You probably need to ",
         "pad your current codes with zeroes.")
  map = fcl_2_cpc2
  out = merge(data.table(fcl = unique(fclCodes)), map, by = "fcl",
              all.x = TRUE)
  setkeyv(out, "fcl")
  out[fclCodes, cpc, allow.cartesian = TRUE]
}
