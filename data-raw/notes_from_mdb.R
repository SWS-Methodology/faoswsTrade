library(dplyr, warn.conflicts = F)
library(stringr)

directory <- file.path("", "mnt", "essdata", "TradeSys", "TradeSys", "Countries")

areas_years <- data.frame(dirnames = dir(directory, full.names = F),
                          stringsAsFactors = F) %>%
  filter(str_detect(dirnames, "^[A-Z]{3}_[1-2][0-9]{3}$")) %>%
  mutate(area = str_extract(dirnames, "^[A-Z]{3}"),
         year = as.integer(str_extract(dirnames, "[1-2][0-9]{3}$")))

areas_years_latest <- areas_years %>%
  group_by(area) %>%
  filter(year == max(year)) %>%
  ungroup()

# Notes table
