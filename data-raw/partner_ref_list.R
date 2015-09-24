unsdpartners <- XLConnect::readWorksheetFromFile("data-raw/Partner reference list.xlsx",
                                             sheet = "countryList")

save(unsdpartners, file = "data/unsdpartners.RData", compress = "xz")

unsdpartnersblocks <- XLConnect::readWorksheetFromFile("data-raw/Partner reference list.xlsx",
                                                 sheet = "statArea")

save(unsdpartnersblocks, file = "data/unsdpartnersblocks.RData", compress = "xz")
