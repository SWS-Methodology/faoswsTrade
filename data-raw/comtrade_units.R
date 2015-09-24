comtradeunits <- read.csv("data-raw/comtrade_units.csv", header = F)
colnames(comtradeunits) <- c("qunit", "wco", "desc", "faoconv")
comtradeunits$faoconv <- NULL
save(comtradeunits, file = "data/comtradeunits.RData")
