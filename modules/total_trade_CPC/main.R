## Total trade

# Year for processing
year <- 2009L

library(faoswsTrade)
library(faosws)
library(stringr)
library(scales)
library(faoswsUtil)
library(tidyr)
library(dplyr, warn.conflicts = F)

completetrade <- ReadDatatable("completed_tf_cpc",
                                where=paste0("timePointYears = '",year,"'")) ## The limit will go away

completetrade <- tbl_df(completetrade)

completetrade <- completetrade %>%
  mutate_(flagTrade = ~ifelse(flagTrade == "I",1,0),
          geographicAreaM49 = ~reportingCountryM49)

total_trade_cpc <- completetrade %>%
  select_(~geographicAreaM49, ~partnerCountryM49, ~timePointYears,
          ~measuredItemCPC, ~measuredElementTrade, ~Value, ~flagTrade) %>%
  group_by_(~geographicAreaM49, ~timePointYears, ~measuredItemCPC, ~measuredElementTrade) %>%
  summarise_each_(funs(sum = sum(., na.rm = TRUE)), vars = c("Value", "flagTrade")) %>%
  ungroup()

total_trade_cpc <- total_trade_cpc %>%
  mutate_(flagTrade = ~ifelse(flagTrade == 1, "I", ""))

SaveData("trade","total_trade_CPC",data.table::as.data.table(total_trade_cpc))


### TO DO FCL
#total_trade_fcl <- total_trade %>%
#  transmute_(geographicAreaM49 = ~reporterM49,
#             measuredElementTrade = ~ifelse(flow == 1,
#                                            "5610",
#                                            ifelse(flow == 2,
#                                                   "5910",
#                                                   NA)),
#             measuredItemFS = ~fcl,
#             timePointYears = ~year,
#             flagObservationStatus = ~flagObservationStatus,
#             flagMethod = ~flagMethod,
#             qty = ~qty,
#             fclunit = ~fclunit,
#             value = ~value)
