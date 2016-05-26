fcl_2_cpc2 = read.csv("data-raw/fcl_2_cpc2.csv", header = T,
                      colClasses = c("character", "character"))

save(fcl_2_cpc2, file = "data/fcl_2_cpc2.RData")
