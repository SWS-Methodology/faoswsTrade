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
  library(faoswsModules)
  SETTINGS = ReadSettings("sws.yml")
  ## If you're not on the system, your settings will overwrite any others
  R_SWS_SHARE_PATH = SETTINGS[["share"]]
  ## Define where your certificates are stored
  faosws::SetClientFiles(SETTINGS[["certdir"]])
  ## Get session information from SWS. Token must be obtained from web interface
  GetTestEnvironment(baseUrl = SETTINGS[["server"]],
                     token = SETTINGS[["token"]])
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

stats <- SaveData("trade","total_trade_CPC",data.table::as.data.table(total_trade_cpc))

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
