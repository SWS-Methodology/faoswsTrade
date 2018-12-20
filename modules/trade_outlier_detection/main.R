
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
library(dplyr)
library(faoswsUtil)
library(faoswsStandardization)
library(faoswsFlag)


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
  switch(geoM49,
         "session" = sessionCountries,
         "all" = selectedCountries)



itemKeys = GetCodeList(domain = "trade", dataset = "total_trade_cpc_m49", "measuredItemCPC")
itemKeys = itemKeys[, code]




#########################################
##### Pull from trade data #####
#########################################

message("Pulling trade Data")

#take geo keys
geoDim = Dimension(name = "geographicAreaM49", keys = selectedGEOCode)

#Define element dimension. These elements are needed to calculate net supply (production + net trade)

eleKeys <- GetCodeList(domain = "trade", dataset = "total_trade_cpc_m49", "measuredElementTrade")
eleKeys = eleKeys[, code]

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




data = GetData(key,omitna = F, normalized=F)
data=normalise(data, areaVar = "geographicAreaM49",
               itemVar = "measuredItemCPC", elementVar = "measuredElementTrade",
               yearVar = "timePointYears", flagObsVar = "flagObservationStatus",
               flagMethodVar = "flagMethod", valueVar = "Value",
               removeNonExistingRecords = F)


trade <- nameData(domain = "trade", dataset = "total_trade_cpc_m49",data)
# trade<- trade %>% group_by(geographicAreaM49,measuredElementTrade,Year) %>% mutate(sumvalue=sum(Value,na.rm=T))
# trade<- trade %>% group_by(geographicAreaM49,measuredElementTrade) %>% mutate(Meansumvalue=mean(sumvalue[Year<2014],na.rm=T))
# trade<- trade %>% group_by(geographicAreaM49,measuredItemCPC,measuredElementTrade) %>% mutate(Meanold=mean(Value[Year<2014],na.rm=T))
# trade<- trade %>% group_by(geographicAreaM49,measuredElementTrademeasuredItemCPC,Year) %>% mutate(relevance=Meanold)
# trade <- trade %>% setDT %>% dcast(geographicAreaM49 + Item + Year ~  Element, value.var = c("Value","Method"))
# trade<- trade %>% group_by(geographicAreaM49,measuredItemCPC,measuredElementTrade) %>% mutate(Meanold=mean(Value[Year<2014],na.rm=T))

trade$Value[trade$Value==0]<-NA # needed to remove NA from the mean, will be restored later

interval<-(startYear-1):(startYear-window)

trade <-trade %>% group_by(geographicAreaM49,measuredItemCPC,measuredElementTrade) %>% mutate(Meanold=mean(Value[timePointYears%in% interval],na.rm=T))
trade <-trade %>% group_by(geographicAreaM49,measuredItemCPC) %>% mutate(MeanoldUV=mean(Value[timePointYears%in% interval & grepl("Unit Value",measuredElementTrade_description)==T],na.rm=T))


trade$Value[is.na(trade$Value)]<-0

# RELEVANCE
#trade <-trade %>% group_by(geographicAreaM49) %>% mutate(sumflow=sum([Year<2014 & Year>2010],na.rm=T))

# imp<-trade[grep("Import",trade$Element),]
# exp<-trade[grep("Export",trade$Element),]

### qty outlier

trade <- trade %>%  mutate(ratio=(Value/Meanold))
# trade <- trade %>%  group_by(geographicAreaM49,measuredItemCPC,timePointYears) %>% mutate(ratioUV=ratio[grepl("Unit Value",measuredElementTrade_description)==T])
#
# trade <- trade %>%   mutate(bigchangeQTY= (ratio>4 | ratio<0.1) & abs(Value-Meanold)>1000 & grepl("Quantity",Element)==T & Year>2013)

trade <- trade %>%   mutate(bigchangeUV= (ratio>4 | ratio<0.1)  & grepl("Unit Value",measuredElementTrade_description)==T & timePointYears>startYear) # these thresholds roughly match the 1th and 99th percentiles of thempirical distributions

outList <- trade %>% filter(bigchangeUV==T)

setDT(outList)

outList[,timePointYears_description := NULL]

bodyOutliers= paste("The Email contains a list of trade outliers based on Unit Value",
                    sep='\n')

sendMailAttachment(outList,"outlierList",bodyOutliers)
#
# imp<-trade[grep("Import",trade$Element),]
# exp<-trade[grep("Export",trade$Element),]
# imp<-imp %>% filter(measuredElementTrade !=5622)
# exp<-exp %>% filter(measuredElementTrade !=5922)
#
#
# imp <- imp %>% group_by(geographicAreaM49,measuredItemCPC,Year) %>% mutate(bigchange_both= any(bigchangeQTY==T) & any(bigchangeUV==T))
# imp <-imp %>% group_by(geographicAreaM49,measuredItemCPC,Year) %>% mutate(Unit_Value=mean(Value[grepl("Unit Value",Element)==T]))
# imp[,16]<-NULL
#
# exp <- exp %>% group_by(geographicAreaM49,measuredItemCPC,Year) %>% mutate(bigchange_both= any(bigchangeQTY==T) & any(bigchangeUV==T))
# exp <-exp %>% group_by(geographicAreaM49,measuredItemCPC,Year) %>% mutate(Unit_Value=mean(Value[grepl("Unit Value",Element)==T]))
# exp[,16]<-NULL
#
#
#
# imp %>%  filter(grepl("Unit Value",Element)==F & bigchange_both==T) %>% write.csv(file="exp_outl.csv")
# exp %>%  filter(grepl("Unit Value",Element)==F & bigchange_both==T) %>% write.csv(file="imp_outl.csv")
#



#prova<- albania %>% group_by(geographicAreaM49,measuredItemCPC,measuredElementTrade,Year) %>%
#  complete(nesting(geographicAreaM49,measuredItemCPC,measuredElementTrade,Year))
