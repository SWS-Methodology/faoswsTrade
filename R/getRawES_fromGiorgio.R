# Feb 2016. Got the raw data from Giorgio via drive

library(dplyr, warn.conflicts = F)
library(data.table)


tldata2000 <- tbl_df(fread("~/Desktop/FAO/Trade/Raw_data/nc201052.dat",
                           header = T, sep = ",")) %>%

  save(fclunits, file = file.path("data", "fclunits.RData"))
