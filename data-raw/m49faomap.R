# Data from Eduard Bukin

m49faomap <- read.table("data-raw/CT.m49_FS.csv",
                        header = TRUE,
                        sep = ",",
                        quote = "\"") %>%
  select(m49 = CT_AreaCode, fao = AreaCode)

save(m49faomap, file = "data/m49faomap.RData")
