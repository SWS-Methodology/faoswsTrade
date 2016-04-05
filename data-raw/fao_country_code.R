fao_country_code <- XLConnect::readWorksheetFromFile("data-raw/FAO_countrycodes.xlsx",
                                                 sheet = "Total")
colnames(fao_country_code) = c("reporter","country_name")

save(fao_country_code, file = "data/faocountrycode.RData")
