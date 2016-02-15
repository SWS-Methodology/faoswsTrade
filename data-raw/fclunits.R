# XLS file taken from Claudia
# Feb2016: new file with additional commodities from Claudia

library(dplyr, warn.conflicts = F)

fclunits <- XLConnect::readWorksheetFromFile(
  #file.path("data-raw",
  #          "HS2007-6 digits Standard_new.xlsx"),
  file.path("data-raw",
            "HS2007-6 digits Standard_new2.xlsx"),
  sheet = 1,
  startCol = 3) %>%
  # Leave only code and unit (drop name, hs code and strange empty column)
  select(fcl = FaoStatCode,
         fclunit = Unit) %>%
  distinct() %>%
  # Convert to integer as I use it in other places
  mutate(fcl = as.integer(fcl),
         # I hate upper case
         fclunit = tolower(fclunit)) %>%
  arrange(fcl)

save(fclunits, file = file.path("data", "fclunits.RData"))
