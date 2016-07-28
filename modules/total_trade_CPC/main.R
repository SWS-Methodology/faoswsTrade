## Total trade

# Year for processing
library(faoswsTrade)
library(faosws)
library(stringr)
library(scales)
library(faoswsUtil)
library(faoswsFlag)
library(tidyr)
library(dplyr, warn.conflicts = F)

local({
  min_versions <- data.frame(package = c("faoswsFlag", "faoswsTrade"),
                             version = c('0.2.4', '0.1.1'),
                             stringsAsFactors = FALSE)

  for (i in nrow(min_versions)){
    # installed version
    p <- packageVersion(min_versions[i,"package"])
    # required version
    v <- package_version(min_versions[i,"version"])
    if(p < v){

      stop(sprintf("%s >= %s required", min_versions[i,"package"], v))
    }
  }

})


if(CheckDebug()){
  library(faoswsModules)
  SETTINGS = ReadSettings("modules/total_trade_CPC/sws.yml")
  ## Define where your certificates are stored
  faosws::SetClientFiles(SETTINGS[["certdir"]])
  ## Get session information from SWS. Token must be obtained from web interface
  GetTestEnvironment(baseUrl = SETTINGS[["server"]],
                     token = SETTINGS[["token"]])
}

year <- as.integer(swsContext.computationParams$year)

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
  mutate_(geographicAreaM49 = ~geographicAreaM49Reporter)

total_trade_cpc <- completetrade %>%
  select_(~geographicAreaM49, ~geographicAreaM49Partner, ~timePointYears,
          ~measuredItemCPC, ~measuredElementTrade, ~Value, ~flagObservationStatus) %>%
  group_by_(~geographicAreaM49, ~timePointYears, ~measuredItemCPC, ~measuredElementTrade) %>%
  summarise_(Value = ~sum(Value, na.rm = TRUE),
             flagObservationStatus = ~aggregateObservationFlag(flagObservationStatus)) %>%
  ungroup()

total_trade_cpc$flagMethod = "s"

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
