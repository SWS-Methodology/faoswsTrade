library(dplyr)

tf_valid <- read.table("data-raw/TF_VALID.csv",
                        header = TRUE,
                        sep = ",",
                        col.names = c("year",
                                      "flow",
                                      "reporter",
                                      "partner",
                                      "fcl",
                                      "quantity",
                                      "value"))


save(tf_valid, file = "data/tf_valid.RData")
