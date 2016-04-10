# Read geonom->fao mapping from mdb-file

#mdblocation <- file.path("",
#                         "mnt",
#                         "essdata",
#                         "TradeSys",
#                         "TradeSys",
#                         "Countries",
#                         "FRA_2012",
#                         "FRA_2012.mdb")


#geonom2fao <- Hmisc::mdb.get(mdblocation,
#               colClasses = c("character", "numeric", "numeric"),
#               stringsAsFactors = F,
#               lowernames = T,
#               tables = "CountryCode") %>%
#  mutate_(code = ~as.integer(code),
#          active = ~ifelse(active == 0, NA, active))

## Update: xls file from Claudia (from the mdb)

library(dplyr, warn.conflicts = F)

geonom2fao <- XLConnect::readWorksheetFromFile(
  file.path("data-raw","FRA_2012_mdb.xls"),
  sheet = 1) %>%
  transmute(code = as.integer(Code),
            faostat = FaoStat,
            active = ifelse(Active == 0, NA, Active)) %>%
  left_join(tbl_df(read.table(file = "data-raw/partner_EN.txt", header = F, sep = "\t", quote = "\"",
                              colClasses = c("integer","integer","integer","character"),
                              nrows = 340, col.names = c("code","year_start","year_end","name"))) %>%
              select_(~code,~name) %>%
              distinct_(~code), by = "code")

save(geonom2fao, file = file.path("data", "geonom2fao.RData"))

geonom2fao %>%
  filter(faostat != active)
