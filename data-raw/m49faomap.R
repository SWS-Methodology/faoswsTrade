# Data from Eduard Bukin

m49faomap <- read.table("data-raw/CT.m49_FS.csv",
                        header = TRUE,
                        sep = ",",
                        quote = "\"") %>%
  select(m49 = CT_AreaCode, fao = AreaCode)

# We change mapping of code m49 490 from fcl 252 to fcl 214
# from unspecified to Taiwan
# http://unstats.un.org/unsd/tradekb/Knowledgebase/Taiwan-Province-of-China-Trade-data?Keywords=China

m49faomap$fao[m49faomap$m49 == 490L] <- 214

# Also we add m49 158 -> fao 214, because for Taiwan
# some countries use 490 and some - 214
# Taiwan m49 code is presented here:
# https://github.com/mkao006/FAOSTATpackage/blob/master/FAOcountryProfile.csv

m49faomap <- rbind(
  m49faomap,
  data.frame(
    m49 = 158,
    fao = 214))


save(m49faomap, file = "data/m49faomap.RData")
