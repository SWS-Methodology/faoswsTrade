library(dplyr)

tfvalid <- read.table("data-raw/TF_VALID.csv",
                        header = TRUE,
                        sep = ",",
                        col.names = c("year",
                                      "flow",
                                      "reporter",
                                      "partner",
                                      "fcl",
                                      "quantity",
                                      "value"))


save(tfvalid, file = "data/tfvalid.RData")
