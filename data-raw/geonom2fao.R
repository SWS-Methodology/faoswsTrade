# Read geonom->fao mapping from mdb-file

mdblocation <- file.path("",
                         "mnt",
                         "essdata",
                         "TradeSys",
                         "TradeSys",
                         "Countries",
                         "FRA_2012",
                         "FRA_2012.mdb")


geonom2fao <- Hmisc::mdb.get(mdblocation,
               colClasses = c("character", "numeric", "numeric"),
               stringsAsFactors = F,
               lowernames = T,
               tables = "CountryCode") %>%
  mutate_(code = ~as.integer(code),
          active = ~ifelse(active == 0, NA, active))

save(geonom2fao, file = file.path("data", "geonom2fao.RData"))

geonom2fao %>%
  filter(faostat != active)
