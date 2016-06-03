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

allReporterKeys <- GetCodeList("trade", "completed_tf_cpc_m49", "geographicAreaM49Reporter")[type == "country", code]
allPartnerKeys <- GetCodeList("trade", "completed_tf_cpc_m49", "geographicAreaM49Partner")[type == "country", code]
allElementKeys <- c("5608", "5609", "5610", "5908", "5909", "5910")
allItemKeys <- GetCodeList("trade", "completed_tf_cpc_m49", "measuredItemCPC")[,code]

completetradekey <- DatasetKey(domain = "trade", dataset = "completed_tf_cpc_m49",
                               dimensions = list(
                                 geographicAreaM49Reporter = Dimension(name = "geographicAreaM49Reporter", keys = allReporterKeys),
                                 geographicAreaM49Partner = Dimension(name = "geographicAreaM49Partner", keys = allPartnerKeys),
                                 measuredElementTrade = Dimension(name = "measuredElementTrade", keys = allElementKeys),
                                 measuredItemCPC = Dimension(name = "measuredItemCPC", keys = allItemKeys),
                                 timePointYears = Dimension(name = "timePointYears", keys = as.character(year))
                               ))

completetrade <- tbl_df(GetData(completetradekey))

completetrade <- completetrade %>%
  mutate_(flagTrade = ~ifelse(flagTrade == "I",1,0),
          geographicAreaM49 = ~geographicAreaM49Reporter)

total_trade_cpc <- completetrade %>%
  select_(~geographicAreaM49, ~geographicAreaM49Partner, ~timePointYears,
          ~measuredItemCPC, ~measuredElementTrade, ~Value, ~flagTrade) %>%
  group_by_(~geographicAreaM49, ~timePointYears, ~measuredItemCPC, ~measuredElementTrade) %>%
  summarise_each_(funs(sum = sum(., na.rm = TRUE)), vars = c("Value", "flagTrade")) %>%
  ungroup()

total_trade_cpc <- total_trade_cpc %>%
  mutate_(flagTrade = ~ifelse(flagTrade == 1, "I", ""))

stats <- SaveData("trade","total_trade_cpc_m49",data.table::as.data.table(total_trade_cpc))

sprintf(
  "Module completed in %1.2f minutes.
  Values inserted: %s
  appended: %s
  ignored: %s
  discarded: %s",
  difftime(Sys.time(), startTime, units = "min"),
  stats[["inserted"]],
  stats[["appended"]],
  stats[["ignored"]],
  stats[["discarded"]]
)

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
