library(dplyr)

EURconversionUSD <- XLConnect::readWorksheetFromFile("data-raw/Euro_exchangerate.xls",
                                                 sheet = "Sheet1")
save(EURconversionUSD, file = "data/EURconversionUSD.RData")
