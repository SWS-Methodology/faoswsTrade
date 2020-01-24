
##'
##' **Author: Carlo Del Bello**
##'
##' **Description:**
##'
##' This module is designed to identify outliers in total trade data
##'
##'
##' **Inputs:**
##'
##' * total trade data


##'
##' **Flag assignment:**
##'
##' None



## load the library
library(faosws)
library(data.table)
library(faoswsUtil)
library(sendmailR)


# ## set up for the test environment and parameters
# R_SWS_SHARE_PATH = Sys.getenv("R_SWS_SHARE_PATH")

if(CheckDebug()){
  message("Not on server, so setting up environment...")

  library(faoswsModules)
  SETT <- ReadSettings("modules/trade_outlier_detection/sws.yml")

  R_SWS_SHARE_PATH <- SETT[["share"]]
  ## Get SWS Parameters
  SetClientFiles(dir = SETT[["certdir"]])
  GetTestEnvironment(
    baseUrl = SETT[["server"]],
    token = SETT[["token"]]
  )
}

startYear = as.numeric(swsContext.computationParams$startYear)
#startYear = as.numeric(2013)

endYear = as.numeric(swsContext.computationParams$endYear)
window = as.numeric(swsContext.computationParams$window)

#endYear = as.numeric(2017)

DEFAULT_THRESHOLD <- 1000L
DEFAULT_RATIO_LOW <- 0.25 # one fourth lower
DEFAULT_RATIO_HIGH <- 4 # four times higher
DEFAULT_GROWTH_LOW <- -0.5 # -50%
DEFAULT_GROWTH_HIGH <- 1 # +100%

interval <- (startYear-1):(startYear-window)

geoM49 = swsContext.computationParams$country_selection
stopifnot(startYear <= endYear)
yearVals = (startYear-window):endYear

##' Get data configuration and session
sessionKey = swsContext.datasets[[1]]

sessionCountries =
  getQueryKey("geographicAreaM49", sessionKey)

geoKeys = GetCodeList(domain = "trade", dataset = "total_trade_cpc_m49",
                      dimension = "geographicAreaM49")[type == "country", code]




##Select the countries based on the user input parameter
selectedGEOCode =
  sessionCountries
#  switch(geoM49,
#         "session" = sessionCountries,
#         "all" = geoKeys)



itemKeys = GetCodeList(domain = "trade", dataset = "total_trade_cpc_m49", "measuredItemCPC")
itemKeys = itemKeys[, code]




#########################################
##### Pull from trade data #####
#########################################

message("TradeOUT: Pulling trade Data")

#take geo keys
geoDim = Dimension(name = "geographicAreaM49", keys = selectedGEOCode)

#Define element dimension. These elements are needed to calculate net supply (production + net trade)

eleKeys <- GetCodeList(domain = "trade", dataset = "total_trade_cpc_m49", "measuredElementTrade")
eleKeys <- eleKeys[, code]

eleDim <- Dimension(name = "measuredElementTrade", keys = eleKeys)

#Define item dimension


itemDim <- Dimension(name = "measuredItemCPC", keys = itemKeys)


# Define time dimension

timeDim <- Dimension(name = "timePointYears", keys = as.character(yearVals))

#Define the key to pull SUA data
key = DatasetKey(domain = "trade", dataset = "total_trade_cpc_m49", dimensions = list(
  geographicAreaM49 = geoDim,
  measuredElementTrade = eleDim,
  measuredItemCPC = itemDim,
  timePointYears = timeDim
))


## To be able to label the outliers based on some threshold over the quantity (if the trade size of a commodity is really low, it is not
## an interesting outlier for us), we will use the trade_outlier_country_thresholds data tables. It contains the threshold identified for each country.
outlier_thresholds <- ReadDatatable("trade_outlier_country_thresholds")

do_not_check <- ReadDatatable("ess_trade_exclude_outlier_check")

data = GetData(key,omitna = FALSE, normalized = FALSE)
data = normalise(data, areaVar = "geographicAreaM49",
               itemVar = "measuredItemCPC", elementVar = "measuredElementTrade",
               yearVar = "timePointYears", flagObsVar = "flagObservationStatus",
               flagMethodVar = "flagMethod", valueVar = "Value",
               removeNonExistingRecords = F)


trade <- nameData(domain = "trade", dataset = "total_trade_cpc_m49", data, except = "timePointYears")

#trade$Value[trade$Value==0]<-NA # needed to remove NA from the mean, will be restored later

trade <- trade[order(geographicAreaM49, measuredItemCPC, measuredElementTrade, timePointYears)]

trade[,
  `:=`(
    meanOld     = mean(Value[timePointYears %in% interval], na.rm = TRUE),
    growth_rate = Value / shift(Value) - 1
  ),
  by = c("geographicAreaM49", "measuredItemCPC", "measuredElementTrade")
]

trade[, flow := substr(measuredElementTrade, 1, 2)]

trade <- merge(trade, outlier_thresholds, by.x = "geographicAreaM49", by.y = "area", all.x = TRUE)

trade[is.na(threshold), threshold := DEFAULT_THRESHOLD]

# XXX
trade[,
  big_qty := meanOld[grepl("Quantity \\[t\\]", measuredElementTrade_description)] > threshold,
  by = c("geographicAreaM49", "measuredItemCPC", "flow", "timePointYears")
]


trade[, ratio := Value / meanOld]

trade[,
  outlier :=
    grepl("Unit Value", measuredElementTrade_description) & # Only UVs
	  !(measuredItemCPC %in% do_not_check$cpc) & # Do not check these
      (!data.table::between(ratio, DEFAULT_RATIO_LOW, DEFAULT_RATIO_HIGH) | # Carlos'
      !data.table::between(growth_rate, DEFAULT_GROWTH_LOW, DEFAULT_GROWTH_HIGH)) & # Growth rates
      big_qty == TRUE & # All need to be big quantities in validated years
      timePointYears >= startYear # Only new years
]

#trade <- trade %>% mutate(bigchangeUV= (ratio > 4 | ratio < 0.1)  & grepl("Unit Value", measuredElementTrade_description)==T & timePointYears>startYear) # these thresholds roughly match the 1th and 99th percentiles of thempirical distributions

outList <-
  trade[
    outlier == TRUE &
    !grepl("\\bn\\.e\\.\\b|\\bnes\\b|alcohol", measuredItemCPC_description)
  ]

outList[, c("flow", "big_qty", "ratio", "outlier", "threshold") := NULL]

outList[, measuredItemCPC := paste0("'", measuredItemCPC)]

bodyOutliers= paste("The Email contains a list of trade outliers based on Unit Value",
                    sep='\n')

sendMailAttachment(outList,"outlierList",bodyOutliers)

