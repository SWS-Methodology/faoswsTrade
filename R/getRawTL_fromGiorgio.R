# Feb 2016. Got the raw data from Giorgio via drive

library(dplyr, warn.conflicts = F)
library(data.table)

# header tl:
## Information about header of UNSD Tariff Line
## http://unstats.un.org/unsd/tradekb/Knowledgebase/UN-Comtrade-Reference-Tables

# pfCode,cmdCode,yr,rtCode,ptCode,rgCode,htCode,estCode,qtCode,NetWeight,TradeQuantity,TradeValue

# CmdCode:	Commodity code
# PfCode:	Commodity classification code (H0, H1, S3: most likely chapter)
# yr:	Year
# rtCode	Reporter Code
# ptCode:	Partner Code
# qtCode:	Quantity code

## ADDITIONAL
# rgCode: Flow (1,2,3,4)
# htCode: ? (0)
# estCode: ? (0,2,4)
# qtCode: qunit (1,2,3,4,5,6,7,8,9,10,11,12,13)

tldata2000 <- tbl_df(fread("~/Desktop/FAO/Trade/Raw_data/tTariffLineData2002.csv",
                           header = T, sep = ",")) %>%

save(fclunits, file = file.path("data", "fclunits.RData"))
