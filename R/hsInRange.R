#' Looks for corresponding FCL codes in country-specific
#' mapping tables from MDB files
#'
#' @export

hsInRange <- function(hs, areacode, flowname, mapdataset,
                      parallel = F) {
  if(!all.equal(length(hs), length(areacode), length(flowname))) stop("Vectors of different length")
  df <- data.frame(hs = hs,
                   areacode = areacode,
                   flowname = flowname,
                   stringsAsFactors = F)
  df_fcl <- plyr::ddply(df,
                        .variables = c("areacode", "flowname"),
                        .fun = function(subdf) {
                          # Subsetting mapping file
                          mapdataset <- mapdataset %>%
                            filter(area == subdf$areacode[1],
                                   flow == subdf$flowname[1])
                          # If no corresponding records in map return empty df
                          if(nrow(mapdataset) == 0) return(data.frame(hs = subdf$hs,
                                                                      fcl = as.integer(NA),
                                                                      stringsAsFactors = F))
                          fcl <- vapply(seq_len(nrow(subdf)),
                                        FUN = function(i) {

                                          hs <- subdf[i, "hs"]

                                          mapdataset <- mapdataset %>%
                                            filter(fromcode <= hs &
                                                     tocode >= hs)
                                          # If no corresponding HS range is available return empty integer
                                          if(nrow(mapdataset) == 0) return(as.integer(NA))
                                          mapdataset %>%
                                            select(fcl) %>%
                                            unlist %>%
                                            '[['(1)
                                        },
                                        FUN.VALUE = integer(1)
                          )
                          data.frame(hs = subdf$hs, fcl = fcl, stringsAsFactors = F)
                        },
                        .parallel = parallel)
  original_n <- nrow(df)
  if(original_n != nrow(df_fcl)) warning("Not equal before joining!")
  df <- df %>% left_join(df_fcl,
                         by = c("hs", "areacode", "flowname"))
  if(nrow(df) != original_n) warning("Not equal after joining!")
  df
}

