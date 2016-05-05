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
if(CheckDebug()){
faosws::SetClientFiles("~/certificates/")
## ADDED COMMENT
faosws::GetTestEnvironment(
  # baseUrl = "https://hqlprswsas1.hq.un.fao.org:8181/sws", # intranet.fao.org/sws
  # baseUrl = "https://hqlprsws2.hq.un.fao.org:8181/sws",
  baseUrl = "https://hqlqasws1.hq.un.fao.org:8181/sws", # QA?
  # token = "349ce2c9-e6bf-485d-8eac-00f6d7183fd6") # Token for QA)
  token = "da889579-5684-4593-aa36-2d86af5d7138") # http://hqlqasws1.hq.un.fao.org:8080/sws/
# token = "f5e52626-a015-4bbc-86d2-6a3b9f70950a") # Second token for QA
#token = token)
}


startTime = Sys.time()

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

paste("Module completed with in",
      round(difftime(Sys.time(), startTime, units = "min"), 2), "minutes.")

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
