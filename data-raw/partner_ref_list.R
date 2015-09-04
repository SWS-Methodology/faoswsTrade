unsdpartners <- XLConnect::readWorksheetFromFile("data-raw/Partner reference list.xlsx",
                                             sheet = "countryList")

save(unsdpartners, file = "data/unsdpartners.rdata", compress = "xz")

unsdpartnersblocks <- XLConnect::readWorksheetFromFile("data-raw/Partner reference list.xlsx",
                                                 sheet = "statArea")

save(unsdpartnersblocks, file = "data/unsdpartnersblocks.rdata", compress = "xz")
