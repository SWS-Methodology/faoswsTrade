##'
##' **Author: Aydan Selek**
##' **Author: Sumeda Siriwardena**
##'
##' **Description:**
##'
##'
##'
##'
##' **Inputs:**
##'
##' * total trade data
##'
##' **Flag assignment:**
##'
##' None


## Load the libraries
knitr::opts_chunk$set(echo = FALSE, eval = FALSE)

library(data.table)
library(faoswsTrade)
library(faosws)
library(stringr)
library(scales)
library(faoswsUtil)
library(faoswsFlag)
library(tidyr)
library(dplyr, warn.conflicts = FALSE)
library(openxlsx)
library(utils)

options(warn=-1)
`%!in%` = Negate(`%in%`)


send_mail <- function(from = NA, to = NA, subject = NA,
                      body = NA, remove = FALSE) {

  if (missing(from)) from <- 'no-reply@fao.org'

  if (missing(to)) {
    if (exists('swsContext.userEmail')) {
      to <- swsContext.userEmail
    }
  }

  if (is.null(to)) {
    stop('No valid email in `to` parameter.')
  }

  if (missing(subject)) stop('Missing `subject`.')

  if (missing(body)) stop('Missing `body`.')

  if (length(body) > 1) {
    body <-
      sapply(
        body,
        function(x) {
          if (file.exists(x)) {
            # https://en.wikipedia.org/wiki/Media_type
            file_type <-
              switch(
                tolower(sub('.*\\.([^.]+)$', '\\1', basename(x))),
                txt  = 'text/plain',
                csv  = 'text/csv',
                png  = 'image/png',
                jpeg = 'image/jpeg',
                jpg  = 'image/jpeg',
                gif  = 'image/gif',
                xls  = 'application/vnd.ms-excel',
                xlsx = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                doc  = 'application/msword',
                docx = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
                pdf  = 'application/pdf',
                zip  = 'application/zip',
                # https://stackoverflow.com/questions/24725593/mime-type-for-serialized-r-objects
                rds  = 'application/octet-stream'
              )

            if (is.null(file_type)) {
              stop(paste(tolower(sub('.*\\.([^.]+)$', '\\1', basename(x))),
                         'is not a supported file type.'))
            } else {
              res <- sendmailR:::.file_attachment(x, basename(x), type = file_type)

              if (remove == TRUE) {
                unlink(x)
              }

              return(res)
            }
          } else {
            return(x)
          }
        }
      )
  } else if (!is.character(body)) {
    stop('`body` should be either a string or a list.')
  }

  sendmailR::sendmail(from, to, subject, as.list(body))
}

R_SWS_SHARE_PATH <- Sys.getenv("R_SWS_SHARE_PATH")

if (CheckDebug()) {

  library(faoswsModules)
  SETTINGS = ReadSettings("modules/trade_commodity_tables/sws.yml")
  ## Define where your certificates are stored
  faosws::SetClientFiles(SETTINGS[["certdir"]])
  ## Get session information from SWS. Token must be obtained from web interface
  GetTestEnvironment(baseUrl = SETTINGS[["server"]],
                     token = SETTINGS[["token"]])

}

# Parameters
min_year = as.numeric(swsContext.computationParams$min_year)
max_year = as.numeric(swsContext.computationParams$max_year)
year <- as.integer(min_year:max_year)
# year <- as.integer(2010:2018)
number_of_year <- as.integer(length(year))

USER <- regmatches(swsContext.username, regexpr("(?<=/).+$", swsContext.username, perl = TRUE))


allm49 <-
  GetCodeList("trade", "total_trade_cpc_m49", "geographicAreaM49")[type == "country", code] %>%
  Dimension(name = "geographicAreaM49", keys = .)

allElementsDim <-
  c( "5610", "5910", "5622", "5922", "5630", "5930",
     "5608", "5609", "5908", "5909", "5638", "5639", "5938", "5939") %>% #,
  ## UV elements:
  #"5638", "5639", "5630", "5938", "5939", "5930") %>%
  Dimension(name = "measuredElementTrade", keys = .)

# CPC codes
cpc_codes_list <- ReadDatatable('commodity_list_for_total_trade_tables')
# cpc_codes <- cpc_codes[is_livestock=='No']
cpc_codes_list <- cpc_codes_list[order(commodity_code)]
cpc_codes <- unique(cpc_codes_list$commodity_code)
animals <- cpc_codes_list[is_livestock=='Yes', commodity_code]


allItemsDim <-
  GetCodeList("trade", "total_trade_cpc_m49", "measuredItemCPC")[code %in%  cpc_codes][,code] %>%
  Dimension(name = "measuredItemCPC", keys = .)

allYearsDim <- Dimension(name = "timePointYears", keys = as.character(year))

totaltradekey <-
  DatasetKey(
    domain = "trade",
    dataset = "total_trade_cpc_m49",
    dimensions =
      list(
        allm49,
        allElementsDim,
        allItemsDim,
        allYearsDim
      )
  )

tradeData <- GetData(totaltradekey) # Trade data

tradeData <- tradeData[!(measuredItemCPC %in% animals & measuredElementTrade %in% c("5610", "5910")),]

continentCodes <- ReadDatatable('continent_country_mapping_for_total_trade_tables') # Continent-country mapping

small_animals <- unique(tradeData[measuredElementTrade=='5609', measuredItemCPC])
big_animals <- unique(tradeData[measuredElementTrade=='5608', measuredItemCPC])
crops <- unique(cpc_codes_list[is_livestock=='No', commodity_code])

timeseriesData_small_animals <- as.data.table(expand.grid(timePointYears = as.character(min_year:max_year),
                                                          geographicAreaM49 = unique(tradeData$geographicAreaM49),
                                                          measuredElementTrade = c(unique(tradeData[measuredItemCPC %in% small_animals, measuredElementTrade]),
                                                                                   "Import - Export", "(Import/Export) - 1", "Status"),
                                                          measuredItemCPC = unique(small_animals)))

timeseriesData_big_animals <- as.data.table(expand.grid(timePointYears = as.character(min_year:max_year),
                                                        geographicAreaM49 = unique(tradeData$geographicAreaM49),
                                                        measuredElementTrade = c(unique(tradeData[measuredItemCPC %in% big_animals, measuredElementTrade]),
                                                                                 "Import - Export", "(Import/Export) - 1", "Status"),
                                                        measuredItemCPC = unique(big_animals)))

timeseriesData_crops <- as.data.table(expand.grid(timePointYears = as.character(min_year:max_year),
                                                  geographicAreaM49 = unique(tradeData$geographicAreaM49),
                                                  measuredElementTrade = c(unique(tradeData[measuredItemCPC %in% crops, measuredElementTrade]),
                                                                           "Import - Export", "(Import/Export) - 1", "Status"),
                                                  measuredItemCPC = unique(crops)))


timeseriesData <- do.call('rbind', list(timeseriesData_crops, timeseriesData_big_animals, timeseriesData_small_animals))


timeseriesData <- merge(timeseriesData, tradeData, by=c("geographicAreaM49","measuredElementTrade","measuredItemCPC", "timePointYears"),all.x = TRUE)

timeseriesData[is.na(Value), Value := 0]

# European Union member countries are included to the also to the Europe continent (Cyprus to Asia)
# In order to avoid double counting, we need threat them only as Eurpean Union and not include to the other country group

european_countries <- unique(continentCodes[country_group=='European Union', m49_country_code])
continentCodes <- continentCodes[m49_country_code %in% european_countries, country_group:= "European Union"]
continentCodes <- unique(continentCodes)
timeseriesData <- merge(timeseriesData, continentCodes, by.x=c("geographicAreaM49"),by.y = c("m49_country_code"), all.x = TRUE)
setnames(timeseriesData, "country_group", "Country Group")

# EX- WAY OF SUMEDA (DOUBLE COUNTED)
# # Europea Union Excluded
# timeseriesData_without_EU <- merge(timeseriesData, continentCodes[country_group != "European Union"], by.x=c("geographicAreaM49"),by.y = c("m49_country_code"), all.x = TRUE)
# timeseriesData_without_EU <- timeseriesData_without_EU[!is.na(country_group)]
# # Only EU
# timeseriesData_with_EU <- merge(timeseriesData, continentCodes[country_group == "European Union"], by.x=c("geographicAreaM49"),by.y = c("m49_country_code"), all.x = TRUE)
# timeseriesData_with_EU <- timeseriesData_with_EU[!is.na(country_group)]
# timeseriesData <- rbind(timeseriesData_with_EU,timeseriesData_without_EU)


# Country wise calculation
timeseriesData[, Value := ifelse(measuredElementTrade %in% c("Import - Export") & measuredItemCPC %in% crops,
                                 round(Value[measuredElementTrade == "5610"] - Value[measuredElementTrade == "5910"], 0), Value),
               by=c("geographicAreaM49","Country Group","measuredItemCPC","timePointYears")]

timeseriesData[, Value := ifelse(measuredElementTrade %in% c("Import - Export") & measuredItemCPC %in% big_animals,
                                 round(Value[measuredElementTrade == "5608"]-Value[measuredElementTrade == "5908"],0), Value),
               by=c("geographicAreaM49","Country Group","measuredItemCPC","timePointYears")]


timeseriesData[, Value := ifelse(measuredElementTrade %in% c("Import - Export") & measuredItemCPC %in% small_animals,
                                 round(Value[measuredElementTrade == "5609"]-Value[measuredElementTrade == "5909"],0), Value),
               by=c("geographicAreaM49","Country Group","measuredItemCPC","timePointYears")]

timeseriesData[, Value := ifelse(measuredElementTrade %in% c("(Import/Export) - 1") & measuredItemCPC %in% crops,
                                 round(((Value[measuredElementTrade == "5610"]/Value[measuredElementTrade == "5910"])-1)*100,0), Value),
               by=c("geographicAreaM49","Country Group","measuredItemCPC","timePointYears")]

timeseriesData[, Value := ifelse(measuredElementTrade %in% c("(Import/Export) - 1") & measuredItemCPC %in% big_animals,
                                 round(((Value[measuredElementTrade == "5608"]/Value[measuredElementTrade == "5908"])-1)*100,0), Value),
               by=c("geographicAreaM49","Country Group","measuredItemCPC","timePointYears")]


timeseriesData[, Value := ifelse(measuredElementTrade %in% c("(Import/Export) - 1") & measuredItemCPC %in% small_animals,
                                 round(((Value[measuredElementTrade == "5609"]/Value[measuredElementTrade == "5909"])-1)*100,0), Value),
               by=c("geographicAreaM49","Country Group","measuredItemCPC","timePointYears")]


timeseriesData[Value == "Inf", Value := NA]
timeseriesData[is.nan(Value), Value := NA]

timeseriesData <- timeseriesData[order(measuredItemCPC, `Country Group`, timePointYears)]

# Calculate the growth of import and export
growthData <- timeseriesData[measuredElementTrade %in% c("5610","5910", "5608","5908","5609","5909")]

growthData <- dcast.data.table(growthData, measuredItemCPC+ timePointYears+ geographicAreaM49+ `Country Group` ~
                                 measuredElementTrade, value.var = c("Value"))

growthData[measuredItemCPC %in% crops, Import_growth := round((`5610` - lag(`5610`))/lag(`5610`),0) , by = c("geographicAreaM49","measuredItemCPC", "Country Group")]

growthData[measuredItemCPC %in% crops, Export_growth := round((`5910` - lag(`5910`))/lag(`5910`),0) , by = c("geographicAreaM49","measuredItemCPC", "Country Group")]

growthData[measuredItemCPC %in% big_animals,
           Import_growth := round((`5608` - lag(`5608`))/lag(`5608`),0) , by = c("geographicAreaM49","measuredItemCPC", "Country Group")]

growthData[measuredItemCPC %in% big_animals,
           Export_growth := round((`5908` - lag(`5908`))/lag(`5908`),0) , by = c("geographicAreaM49","measuredItemCPC", "Country Group")]

growthData[measuredItemCPC %in% small_animals,
           Import_growth := round((`5609` - lag(`5609`))/lag(`5609`),0) , by = c("geographicAreaM49","measuredItemCPC", "Country Group")]

growthData[measuredItemCPC %in% small_animals,
           Export_growth := round((`5909` - lag(`5909`))/lag(`5909`),0) , by = c("geographicAreaM49","measuredItemCPC", "Country Group")]


growthData[, c("5610", "5910", "5608", "5908", "5609", "5909") := NULL]

growthData<- melt.data.table(growthData, id.vars = c("measuredItemCPC","timePointYears","geographicAreaM49","Country Group"),
                             measure.vars=c("Import_growth","Export_growth"),
                             value.name= "Value")

setnames(growthData, "variable", "measuredElementTrade")

growthData[is.nan(Value), Value := NA]

growthData[, c("flagObservationStatus", "flagMethod"):= NA]

timeseriesData <- rbind(timeseriesData,growthData)

timeseriesData[Value == "Inf", Value := NA]

timeseriesData[measuredElementTrade == "Status", Value := NA]

timeseriesData <-
  timeseriesData[
    measuredElementTrade == "(Import/Export) - 1",
    .(geographicAreaM49,`Country Group`, measuredItemCPC, timePointYears, s = between(Value, -5, 5))
    ][
      timeseriesData,
      on = c("geographicAreaM49", "Country Group","measuredItemCPC","timePointYears")
      ][
        measuredElementTrade == "Status", Value := s * 1
        ][,
          s := NULL
          ]


timeseriesData[, Value:= round(Value, 0)] # Country wise calculation will be reasumed

# Region wise calculation
timeseriesDataRegion <- copy(timeseriesData)
timeseriesDataRegion <- subset(timeseriesDataRegion, measuredElementTrade %in% c("5610", "5910", "5622", "5922", "5630", "5930",
                                                                                 "5608", "5609", "5908", "5909", "5638", "5938", "5639", "5939",
                                                                                 "Import - Export", "(Import/Export) - 1", "Status") )

timeseriesDataRegion[, geographicAreaM49:= NULL]

timeseriesDataRegion[, c("flagObservationStatus", "flagMethod"):= NULL]

timeseriesDataRegion[, Value := ifelse(measuredElementTrade %in% c("Import - Export", "(Import/Export) - 1", "5630", "5930","5638","5938","5639","5939", "Status"), NA,Value),
                     by=c("Country Group", "measuredItemCPC", "timePointYears")]


# Aggregate the Quantity and Value elements
timeseriesDataRegion[, Agg_Sum := ifelse(measuredElementTrade %in% c("5610","5910", "5608","5609","5908","5909","5622","5922"), sum(Value), Value),
                     by = list(measuredItemCPC, `Country Group`, measuredElementTrade, timePointYears)]

timeseriesDataRegion[, c("Value"):= NULL]
timeseriesDataRegion <- unique(timeseriesDataRegion)

setnames(timeseriesDataRegion, c("Agg_Sum"),c("Value"))

# Manage other ad-hoc elements for tyhe regional calculation
timeseriesDataRegion[, Value:= ifelse(measuredElementTrade %in% c("Import - Export") & measuredItemCPC %in% crops,
                                      round(Value[measuredElementTrade == "5610"]-Value[measuredElementTrade == "5910"],0), Value),
                     by=c("Country Group", "measuredItemCPC", "timePointYears")]

timeseriesDataRegion[, Value := ifelse(measuredElementTrade %in% c("Import - Export") & measuredItemCPC %in% big_animals,
                                       round(Value[measuredElementTrade == "5608"]-Value[measuredElementTrade == "5908"],0), Value),
                     by=c("Country Group","measuredItemCPC","timePointYears")]


timeseriesDataRegion[, Value := ifelse(measuredElementTrade %in% c("Import - Export") & measuredItemCPC %in% small_animals,
                                       round(Value[measuredElementTrade == "5609"]-Value[measuredElementTrade == "5909"],0), Value),
                     by=c("Country Group","measuredItemCPC","timePointYears")]


timeseriesDataRegion[, Value:= ifelse(measuredElementTrade %in% c("5630") & measuredItemCPC %in% crops,
                                      round((Value[measuredElementTrade == "5622"]*1000)/Value[measuredElementTrade == "5610"],0), Value),
                     by=c("Country Group","measuredItemCPC" ,"timePointYears")]

timeseriesDataRegion[, Value := ifelse(measuredElementTrade %in% c("5638") & measuredItemCPC %in% big_animals,
                                       round((Value[measuredElementTrade == "5622"]*1000)/Value[measuredElementTrade == "5608"],0), Value),
                     by=c("Country Group","measuredItemCPC" ,"timePointYears")]

timeseriesDataRegion[, Value := ifelse(measuredElementTrade %in% c("5639") & measuredItemCPC %in% small_animals,
                                       round((Value[measuredElementTrade == "5622"]*1000)/Value[measuredElementTrade == "5609"],0), Value),
                     by=c("Country Group","measuredItemCPC" ,"timePointYears")]

timeseriesDataRegion[, Value:= ifelse(measuredElementTrade %in% c("5930") & measuredItemCPC %in% crops,
                                      round((Value[measuredElementTrade == "5922"]*1000)/Value[measuredElementTrade == "5910"],0), Value),
                     by=c("Country Group","measuredItemCPC" ,"timePointYears")]

timeseriesDataRegion[, Value := ifelse(measuredElementTrade %in% c("5938") & measuredItemCPC %in% big_animals,
                                       round((Value[measuredElementTrade == "5922"]*1000)/Value[measuredElementTrade == "5908"],0), Value),
                     by=c("Country Group","measuredItemCPC" ,"timePointYears")]

timeseriesDataRegion[, Value := ifelse(measuredElementTrade %in% c("5939") & measuredItemCPC %in% small_animals,
                                       round((Value[measuredElementTrade == "5922"]*1000)/Value[measuredElementTrade == "5909"],0), Value),
                     by=c("Country Group","measuredItemCPC" ,"timePointYears")]

timeseriesDataRegion[, Value:= ifelse(measuredElementTrade %in% c("(Import/Export) - 1") & measuredItemCPC %in% crops,
                                      round(((Value[measuredElementTrade == "5610"]/Value[measuredElementTrade == "5910"])-1)*100,0), Value),
                     by=c("Country Group","measuredItemCPC","timePointYears")]

timeseriesDataRegion[, Value := ifelse(measuredElementTrade %in% c("(Import/Export) - 1")& measuredItemCPC %in% big_animals,
                                       round(((Value[measuredElementTrade == "5608"]/Value[measuredElementTrade == "5908"])-1)*100,0), Value),
                     by=c("Country Group","measuredItemCPC","timePointYears")]

timeseriesDataRegion[, Value := ifelse(measuredElementTrade %in% c("(Import/Export) - 1")& measuredItemCPC %in% small_animals,
                                       round(((Value[measuredElementTrade == "5609"]/Value[measuredElementTrade == "5909"])-1)*100,0), Value),
                     by=c("Country Group","measuredItemCPC","timePointYears")]


timeseriesDataRegion <- timeseriesDataRegion[order(measuredItemCPC, `Country Group`, timePointYears)]

growthDataRe <- timeseriesDataRegion[measuredElementTrade %in% c("5610","5910", "5608", "5908", "5609", "5909")]

growthDataRe <- dcast.data.table(growthDataRe, measuredItemCPC+timePointYears+`Country Group` ~ measuredElementTrade, value.var = c("Value"))

growthDataRe[measuredItemCPC %in% crops, Import_growth := round(((`5610` - lag(`5610`))/lag(`5610`))*100, 0) , by = c("measuredItemCPC", "Country Group")]

growthDataRe[measuredItemCPC %in% crops, Export_growth := round(((`5910` - lag(`5910`))/lag(`5910`))*100, 0) , by = c("measuredItemCPC", "Country Group")]

growthDataRe[measuredItemCPC %in% big_animals,
             Import_growth := round(((`5608` - lag(`5608`))/lag(`5608`))*100,0) , by = c("measuredItemCPC", "Country Group")]

growthDataRe[measuredItemCPC %in% big_animals,
             Export_growth := round(((`5908` - lag(`5908`))/lag(`5908`))*100,0) , by = c("measuredItemCPC", "Country Group")]

growthDataRe[measuredItemCPC %in% small_animals,
             Import_growth := round((`5609` - lag(`5609`))/lag(`5609`),0) , by = c("measuredItemCPC", "Country Group")]

growthDataRe[measuredItemCPC %in% small_animals,
             Export_growth := round((`5909` - lag(`5909`))/lag(`5909`),0) , by = c("measuredItemCPC", "Country Group")]


growthDataRe[, c("5610","5910", "5608","5908","5609","5909") := NULL]

growthDataRe<- melt.data.table(growthDataRe, id.vars = c("measuredItemCPC","timePointYears","Country Group"),
                               measure.vars=c("Import_growth","Export_growth"), value.name= "Value")

setnames(growthDataRe, "variable", "measuredElementTrade")

timeseriesDataRegion <- rbind(timeseriesDataRegion, growthDataRe)

timeseriesDataRegion[Value == "Inf", Value := NA]
timeseriesDataRegion[is.nan(Value), Value := NA]


timeseriesDataRegion <-
  timeseriesDataRegion[
    measuredElementTrade == "(Import/Export) - 1",
    .(`Country Group`, measuredItemCPC, timePointYears, s = between(Value, -5, 5))
    ][
      timeseriesDataRegion,
      on = c("Country Group","measuredItemCPC","timePointYears")
      ][
        measuredElementTrade == "Status", Value := s * 1
        ][,
          s := NULL
          ]


timeseriesDataRegion[measuredElementTrade == "5610", measuredElementTrade := "Import_Quantity (t)"]
timeseriesDataRegion[measuredElementTrade == "5910", measuredElementTrade := "Export_Quantity (t)"]
timeseriesDataRegion[measuredElementTrade == "5608", measuredElementTrade := "Import_Quantity [head]"]
timeseriesDataRegion[measuredElementTrade == "5908", measuredElementTrade := "Export_Quantity [head]"]
timeseriesDataRegion[measuredElementTrade == "5609", measuredElementTrade := "Import_Quantity [1000 head]"]
timeseriesDataRegion[measuredElementTrade == "5909", measuredElementTrade := "Export_Quantity [1000 head]"]
timeseriesDataRegion[measuredElementTrade == "5622", measuredElementTrade:= "Import Value [1000 $]"]
timeseriesDataRegion[measuredElementTrade == "5922", measuredElementTrade:= "Export Value [1000 $]"]
timeseriesDataRegion[measuredElementTrade == "5630", measuredElementTrade:= "Import UV [$/t]"]
timeseriesDataRegion[measuredElementTrade == "5930", measuredElementTrade:= "Export UV [$/t]"]
timeseriesDataRegion[measuredElementTrade == "5638", measuredElementTrade := "Import UV [$/head]"]
timeseriesDataRegion[measuredElementTrade == "5938", measuredElementTrade := "Export UV [$/head]"]
timeseriesDataRegion[measuredElementTrade == "5639", measuredElementTrade := "Import UV [$/1000 head]"]
timeseriesDataRegion[measuredElementTrade == "5939", measuredElementTrade := "Export UV [$/1000 head]"]

setnames(timeseriesDataRegion, "measuredItemCPC", "measuredItemFbsSua")

timeseriesDataRegion <- nameData("sua-fbs", "sua_unbalanced", timeseriesDataRegion) # Name SWS keys

timeseriesDataRegion[, c("timePointYears_description"):= NULL]
timeseriesDataRegion[, Value:= round(Value,0)]
setnames(timeseriesDataRegion, c("measuredItemFbsSua","measuredItemFbsSua_description"), c("Commodity CPC Code", "Commodity name"))


timeseriesDataRegion <- dcast.data.table(timeseriesDataRegion, `Country Group`+ `Commodity CPC Code`+ `Commodity name`+ measuredElementTrade
                                         ~ timePointYears, value.var = c("Value"))


setnames(timeseriesDataRegion, c("measuredElementTrade"), c("Trade Dimension"))

timeseriesDataRegion[, Country:= NA]

setcolorder(timeseriesDataRegion, c("Country Group","Country","Commodity name","Commodity CPC Code","Trade Dimension"
                                    ,c(as.character(year))))


timeseriesDataRegion[, `Trade Dimension`:= as.character(`Trade Dimension`)] # Region wise calculation finalised

# World wise calculation
world <- subset(timeseriesData, measuredElementTrade %in% c("5610","5910","5622","5922","5630","5930",
                                                            "5608","5609","5908","5909","5638","5938","5639","5939",
                                                            "Import - Export", "(Import/Export) - 1","Status") )

world[, c("flagObservationStatus", "flagMethod"):=NULL]

world <- world[!duplicated(world[, c("geographicAreaM49", "measuredItemCPC", "timePointYears", "measuredElementTrade"), with = FALSE])]

world[, Value:= ifelse(measuredElementTrade %in% c("Import - Export","(Import/Export) - 1","5630","5930","Status"), NA,Value),
      by=c("Country Group","measuredItemCPC","timePointYears")]

world[, geographicAreaM49 :=NULL]

world[, `Country Group`:= c("World")]

# Aggregate the Quantity and Value elements
world[, Agg_Sum := ifelse(measuredElementTrade %in% c("5610","5910", "5608","5609","5908","5909", "5622","5922"), sum(Value), Value),
      by = list(measuredItemCPC,`Country Group`, measuredElementTrade,timePointYears)]

world[,c("Value") := NULL]
world <- unique(world)
setnames(world, c("Agg_Sum"),c("Value"))

# Manage other ad-hoc elements for tyhe regional calculation
world[, Value:= ifelse(measuredElementTrade %in% c("Import - Export") & measuredItemCPC %in% crops,
                       round(Value[measuredElementTrade == "5610"]-Value[measuredElementTrade == "5910"],0), Value),
      by=c("Country Group","measuredItemCPC","timePointYears")]

world[, Value := ifelse(measuredElementTrade %in% c("Import - Export") & measuredItemCPC %in% big_animals,
                        round(Value[measuredElementTrade == "5608"]-Value[measuredElementTrade == "5908"],0), Value),
      by=c("Country Group","measuredItemCPC","timePointYears")]

world[, Value := ifelse(measuredElementTrade %in% c("Import - Export") & measuredItemCPC %in% small_animals,
                        round(Value[measuredElementTrade == "5609"]-Value[measuredElementTrade == "5909"],0), Value),
      by=c("Country Group","measuredItemCPC","timePointYears")]

world[, Value:= ifelse(measuredElementTrade %in% c("5630") & measuredItemCPC %in% crops,
                       round((Value[measuredElementTrade == "5622"]*1000)/Value[measuredElementTrade == "5610"],0), Value),
      by=c("Country Group","measuredItemCPC" ,"timePointYears")]

world[, Value := ifelse(measuredElementTrade %in% c("5638") & measuredItemCPC %in% big_animals,
                        round((Value[measuredElementTrade == "5622"]*1000)/Value[measuredElementTrade == "5608"],0), Value),
      by=c("Country Group","measuredItemCPC" ,"timePointYears")]

world[, Value := ifelse(measuredElementTrade %in% c("5639") & measuredItemCPC %in% small_animals,
                        round((Value[measuredElementTrade == "5622"]*1000)/Value[measuredElementTrade == "5609"],0), Value),
      by=c("Country Group","measuredItemCPC" ,"timePointYears")]

world[, Value:= ifelse(measuredElementTrade %in% c("5930") & measuredItemCPC %in% crops,
                       round((Value[measuredElementTrade == "5922"]*1000)/Value[measuredElementTrade == "5910"],0), Value),
      by=c("Country Group","measuredItemCPC" ,"timePointYears")]

world[, Value := ifelse(measuredElementTrade %in% c("5938") & measuredItemCPC %in% big_animals,
                        round((Value[measuredElementTrade == "5922"]*1000)/Value[measuredElementTrade == "5908"],0), Value),
      by=c("Country Group","measuredItemCPC" ,"timePointYears")]

world[, Value := ifelse(measuredElementTrade %in% c("5939") & measuredItemCPC %in% small_animals,
                        round((Value[measuredElementTrade == "5922"]*1000)/Value[measuredElementTrade == "5909"],0), Value),
      by=c("Country Group","measuredItemCPC" ,"timePointYears")]

world[, Value:= ifelse(measuredElementTrade %in% c("(Import/Export) - 1") & measuredItemCPC %in% crops,
                       round(((Value[measuredElementTrade == "5610"]/Value[measuredElementTrade == "5910"])-1)*100), Value),
      by=c("Country Group","measuredItemCPC","timePointYears")]

world[, Value := ifelse(measuredElementTrade %in% c("(Import/Export) - 1")& measuredItemCPC %in% big_animals,
                        round(((Value[measuredElementTrade == "5608"]/Value[measuredElementTrade == "5908"])-1)*100,0), Value),
      by=c("Country Group","measuredItemCPC","timePointYears")]

world[, Value := ifelse(measuredElementTrade %in% c("(Import/Export) - 1")& measuredItemCPC %in% small_animals,
                        round(((Value[measuredElementTrade == "5609"]/Value[measuredElementTrade == "5909"])-1)*100,0), Value),
      by=c("Country Group","measuredItemCPC","timePointYears")]


world <- world[order(measuredItemCPC, `Country Group`, timePointYears)]

growthWorld <- world[measuredElementTrade %in% c("5608","5908","5609","5909","5610","5910")]

growthWorld <- dcast.data.table(growthWorld, measuredItemCPC+timePointYears+`Country Group` ~ measuredElementTrade, value.var = c("Value"))

growthWorld[measuredItemCPC %in% crops, Import_growth:= round(((`5610` - lag(`5610`))/lag(`5610`))*100,0) , by = c("measuredItemCPC", "Country Group")]

growthWorld[measuredItemCPC %in% crops, Export_growth:= round(((`5910` - lag(`5910`))/lag(`5910`))*100,0) , by = c("measuredItemCPC", "Country Group")]

growthWorld[measuredItemCPC %in% big_animals,
            Import_growth := round(((`5608` - lag(`5608`))/lag(`5608`))*100,0) , by = c("measuredItemCPC", "Country Group")]

growthWorld[measuredItemCPC %in% big_animals,
            Export_growth := round(((`5908` - lag(`5908`))/lag(`5908`))*100,0) , by = c("measuredItemCPC", "Country Group")]

growthWorld[measuredItemCPC %in% small_animals,
            Import_growth := round((`5609` - lag(`5609`))/lag(`5609`),0) , by = c("measuredItemCPC", "Country Group")]

growthWorld[measuredItemCPC %in% small_animals,
            Export_growth := round((`5909` - lag(`5909`))/lag(`5909`),0) , by = c("measuredItemCPC", "Country Group")]


growthWorld[, c("5610","5910", "5608","5908","5609","5909") := NULL]

growthWorld<- melt.data.table(growthWorld, id.vars = c("measuredItemCPC","timePointYears","Country Group"),
                              measure.vars=c("Import_growth","Export_growth"), value.name = "Value")

setnames(growthWorld, "variable", "measuredElementTrade")

world <- rbind(world,growthWorld)
world[ is.nan(Value), Value := NA]
world[ Value == "Inf" , Value := NA]

world <-
  world[
    measuredElementTrade == "(Import/Export) - 1",
    .(`Country Group`, measuredItemCPC, timePointYears, s = between(Value, -5, 5))
    ][
      world,
      on = c("Country Group","measuredItemCPC","timePointYears")
      ][
        measuredElementTrade == "Status", Value := s * 1
        ][,
          s := NULL
          ]

world[measuredElementTrade == "5610", measuredElementTrade := "Import_Quantity (t)"]
world[measuredElementTrade == "5910", measuredElementTrade := "Export_Quantity (t)"]
world[measuredElementTrade == "5608", measuredElementTrade := "Import_Quantity [head]"]
world[measuredElementTrade == "5908", measuredElementTrade := "Export_Quantity [head]"]
world[measuredElementTrade == "5609", measuredElementTrade := "Import_Quantity [1000 head]"]
world[measuredElementTrade == "5909", measuredElementTrade := "Export_Quantity [1000 head]"]
world[measuredElementTrade == "5622", measuredElementTrade := "Import Value [1000 $]"]
world[measuredElementTrade == "5922", measuredElementTrade := "Export Value [1000 $]"]
world[measuredElementTrade == "5630", measuredElementTrade := "Import UV [$/t]"]
world[measuredElementTrade == "5930", measuredElementTrade := "Export UV [$/t]"]
world[measuredElementTrade == "5638", measuredElementTrade := "Import UV [$/head]"]
world[measuredElementTrade == "5938", measuredElementTrade := "Export UV [$/head]"]
world[measuredElementTrade == "5639", measuredElementTrade := "Import UV [$/1000 head]"]
world[measuredElementTrade == "5939", measuredElementTrade := "Export UV [$/1000 head]"]

setnames(world,"measuredItemCPC","measuredItemFbsSua")

world <- nameData("sua-fbs", "sua_unbalanced",world)
world[, c("timePointYears_description") := NULL]
world[, Value := round(Value,0)]
setnames(world, c("measuredItemFbsSua", "measuredItemFbsSua_description"), c("Commodity CPC Code", "Commodity name"))

world <- dcast.data.table(world, `Country Group`+ `Commodity CPC Code`+ `Commodity name`+ measuredElementTrade
                          ~ timePointYears, value.var = c("Value"))

setnames(world, c( "measuredElementTrade"), c("Trade Dimension"))
world[,Country:= NA]

setcolorder(world, c("Country Group","Country","Commodity name","Commodity CPC Code","Trade Dimension",
                     c(as.character(year))))

world[, `Trade Dimension` := as.character(`Trade Dimension`)] # World wise calculation finalised

# Country wise calculation resume
country <- copy(timeseriesData)

country[measuredElementTrade == "5610", measuredElementTrade := "Import_Quantity (t)"]
country[measuredElementTrade == "5910", measuredElementTrade := "Export_Quantity (t)"]
country[measuredElementTrade == "5608", measuredElementTrade := "Import_Quantity [head]"]
country[measuredElementTrade == "5908", measuredElementTrade := "Export_Quantity [head]"]
country[measuredElementTrade == "5609", measuredElementTrade := "Import_Quantity [1000 head]"]
country[measuredElementTrade == "5909", measuredElementTrade := "Export_Quantity [1000 head]"]
country[measuredElementTrade == "5622", measuredElementTrade := "Import Value [1000 $]"]
country[measuredElementTrade == "5922", measuredElementTrade := "Export Value [1000 $]"]
country[measuredElementTrade == "5630", measuredElementTrade := "Import UV [$/t]"]
country[measuredElementTrade == "5930", measuredElementTrade := "Export UV [$/t]"]
country[measuredElementTrade == "5638", measuredElementTrade := "Import UV [$/head]"]
country[measuredElementTrade == "5938", measuredElementTrade := "Export UV [$/head]"]
country[measuredElementTrade == "5639", measuredElementTrade := "Import UV [$/1000 head]"]
country[measuredElementTrade == "5939", measuredElementTrade := "Export UV [$/1000 head]"]

setnames(country, "measuredItemCPC", "measuredItemFbsSua")

country <- nameData("sua-fbs", "sua_unbalanced", country)
country[, c("timePointYears_description"):= NULL]
country[, Value:= round(Value,0)]
setnames(country, c("measuredItemFbsSua", "measuredItemFbsSua_description"), c("Commodity CPC Code", "Commodity name"))

country[, c("geographicAreaM49"):= NULL]

country <- dcast.data.table(country, geographicAreaM49_description + `Country Group`+ `Commodity CPC Code`+ `Commodity name`+ measuredElementTrade
                            ~ timePointYears, value.var = c("Value", "flagObservationStatus", "flagMethod"))

setnames(country, c( "measuredElementTrade", "geographicAreaM49_description"),c("Trade Dimension","Country"))


yearcols <- grep("^Value", names(country), value = TRUE)
yearcols_new = gsub("^.*?_","", yearcols)

flagcols <- grep("^flagObservationStatus", names(country), value = TRUE)
flagcols_new=gsub("_", " ", flagcols, fixed=TRUE)

methodcols <- grep("^flagMethod", names(country), value = TRUE)
methodcols_new=gsub("_", " ", methodcols, fixed=TRUE)

addorder <- as.vector(rbind(yearcols_new, flagcols_new, methodcols_new))

setnames(country, c(yearcols, flagcols, methodcols), c(yearcols_new, flagcols_new, methodcols_new))

setcolorder(country, c("Country Group", "Country", "Commodity name", "Commodity CPC Code", "Trade Dimension", addorder))

flagcols_new <- grep("^flagObservationStatus", names(country), value = TRUE)

methodcols_new <- grep("^flagMethod", names(country), value = TRUE)

setnames(country, flagcols_new, rep("Status", number_of_year))
setnames(country, methodcols_new, rep("Method", number_of_year))

country[, `Trade Dimension`:= as.character(`Trade Dimension`)]

country <- subset(country, `Trade Dimension` %in% c("Import_Quantity (t)", "Export_Quantity (t)", "Import Value [1000 $]",
                                                    "Export Value [1000 $]", "Import UV [$/t]","Export UV [$/t]",
                                                    "Import_Quantity [head]", "Export_Quantity [head]","Import_Quantity [1000 head]","Export_Quantity [1000 head]",
                                                    "Import UV [$/head]","Export UV [$/head]" ,
                                                    "Import UV [$/1000 head]","Export UV [$/1000 head]"))

# Give the last shape of data and produce excel files
message("Excel files producing...")
# timeseriesDataRegion : regional data
# country              : country data
# world                : world data

world[`Trade Dimension` == "(Import/Export) - 1", `Trade Dimension` := "[(Import/Export) - 1] in %"]
world[`Trade Dimension` == "Import_growth", `Trade Dimension` := "[Import_growth] in %"]
world[`Trade Dimension` == "Export_growth", `Trade Dimension` := "[Export_growth] in % "]

timeseriesDataRegion[`Trade Dimension` == "(Import/Export) - 1", `Trade Dimension` := "[(Import/Export) - 1] in %"]
timeseriesDataRegion[`Trade Dimension` == "Import_growth", `Trade Dimension` := "[Import_growth] in %"]
timeseriesDataRegion[`Trade Dimension` == "Export_growth", `Trade Dimension` := "[Export_growth] in % "]

country[`Trade Dimension` == "(Import/Export) - 1", `Trade Dimension` := "[(Import/Export) - 1] in %"]
country[`Trade Dimension` == "Import_growth", `Trade Dimension` := "[Import_growth] in %"]
country[`Trade Dimension` == "Export_growth", `Trade Dimension` := "[Export_growth] in % "]

foritem_names <- country[,.(`Commodity name`)]
foritem_names[`Commodity name`=='Swine / pigs', `Commodity name`:= 'Swine-pigs']
item_names <- unique(foritem_names$`Commodity name`)
# Create temporary location for the output
TMP_DIR <- file.path(tempdir())
if (!file.exists(TMP_DIR)) dir.create(TMP_DIR, recursive = TRUE)
tmp_file_commoditytables <- file.path(TMP_DIR, paste0("item_", item_names,".xlsx"))


# tmp_file_commoditytables <- file.path('C:/Users/Selek/Desktop/ALL-2021/', paste0("item_", item_names,".xlsx"))

# Item files is being producing
list_of_commodity <- unique(world$`Commodity CPC Code`)
for (i in 1:length(list_of_commodity)){

  item_name <- unique(country[, c("Commodity name","Commodity CPC Code"), with = FALSE])
  if(list_of_commodity[i] == "02140"){

    item_name <- c("Swine_Pigs")
  }else{

    item_name <- unique(item_name[`Commodity CPC Code` == list_of_commodity[i]]$`Commodity name`)

  }
  # TMP_DIR <- file.path(tempdir())
  # if (!file.exists(TMP_DIR)) dir.create(TMP_DIR, recursive = TRUE)
  # tmp_file_commoditytables <- file.path(TMP_DIR, paste0("item_", item_name,".xlsx"))

  x1 <- subset(world, `Commodity CPC Code` == list_of_commodity[i])

  if (list_of_commodity[i] %in% crops){

    z <- c("Import_Quantity (t)","Import Value [1000 $]", "Import UV [$/t]", "Export_Quantity (t)", "Export Value [1000 $]","Export UV [$/t]","Import - Export",
           "[(Import/Export) - 1] in %", "[Import_growth] in %", "[Export_growth] in % ", "Status" )

  } else if (list_of_commodity[i] %in% big_animals) {

    z <- c("Import_Quantity [head]","Import Value [1000 $]", "Import UV [$/head]", "Export_Quantity [head]", "Export Value [1000 $]","Export UV [$/head]","Import - Export",
           "[(Import/Export) - 1] in %", "[Import_growth] in %", "[Export_growth] in % ", "Status" )

  } else {

    z <- c("Import_Quantity [1000 head]","Import Value [1000 $]", "Import UV [$/1000 head]", "Export_Quantity [1000 head]",
           "Export Value [1000 $]","Export UV [$/1000 head]","Import - Export",
           "[(Import/Export) - 1] in %", "[Import_growth] in %", "[Export_growth] in % ", "Status" )

  }

  x1 <- x1[order(match(`Trade Dimension`, z)),]


  x1_1 <- rbind(x1[1:3,],x1[1:3,][nrow(x1[1:3,]) + 1L])
  x1_2 <- rbind(x1[4:6,],x1[4:6,][nrow(x1[4:6,]) + 1L])
  x1_3 <- rbind(x1[7:11,],x1[7:11,][nrow(x1[7:11,]) + 1L])

  x1 <- rbind(x1_1,x1_2,x1_3)

  x1[is.na(x1)] <- ""

  numeric_columns  <- grep("^[[:digit:]]{4}$", names(x1), value = TRUE)
  x1[, (numeric_columns) := lapply(.SD, as.numeric), .SDcols = numeric_columns] # World

  # Country details

  if (list_of_commodity[i] %in% crops){

    z2 <- c("Import_Quantity (t)","Import Value [1000 $]", "Import UV [$/t]", "Export_Quantity (t)", "Export Value [1000 $]","Export UV [$/t]")

  } else if (list_of_commodity[i] %in% big_animals) {

    z2 <- c("Import_Quantity [head]","Import Value [1000 $]", "Import UV [$/head]", "Export_Quantity [head]", "Export Value [1000 $]","Export UV [$/head]")

  } else {

    z2 <- c("Import_Quantity [1000 head]","Import Value [1000 $]", "Import UV [$/1000 head]", "Export_Quantity [1000 head]", "Export Value [1000 $]","Export UV [$/1000 head]")
  }

  x2 <- subset(country, `Commodity CPC Code` == list_of_commodity[i] & `Trade Dimension` %in% z2)

  x2 <- x2[order(match(`Trade Dimension`, z2)),]
  x2 <- x2[order(Country),]

  # Regions
  x3 <- subset(timeseriesDataRegion, `Commodity CPC Code` == list_of_commodity[i])
  x3 <- x3[order(match(`Trade Dimension`, z)),]
  xxx <- list()

  for (j in 1:length(unique(x3$`Country Group`))){

    xx <- subset(x3, `Country Group` == unique(x3$`Country Group`)[j])
    xx <- xx[order(match(`Trade Dimension`, z)),]

    xx_1<-rbind(xx[1:3,],xx[1:3,][nrow(xx[1:3,]) + 1L])
    xx_2 <-rbind(xx[4:6,],xx[4:6,][nrow(xx[4:6,]) + 1L])
    xx_3 <-rbind(xx[7:11,],xx[7:11,][nrow(xx[7:11,]) + 1L])


    xx <- rbind(xx_1,xx_2,xx_3)

    xx[is.na(xx)] <- ""

    numeric_columns  <- grep("^[[:digit:]]{4}$", names(xx), value = TRUE)
    xx[, (numeric_columns) := lapply(.SD, as.numeric), .SDcols = numeric_columns]


    xxx[[j]] <- xx

  }

  x3 <- rbindlist(xxx) # Regions


  wb <- createWorkbook("Creator of workbook")
  addWorksheet(wb, sheetName = "World_summary")
  addWorksheet(wb, sheetName = "Country_details")
  addWorksheet(wb, sheetName = "Regions")

  header_st <- createStyle(textDecoration = "Bold")

  writeData(wb, "World_summary", x1, headerStyle = header_st)
  writeData(wb, "Country_details", x2, headerStyle = header_st)
  writeData(wb, "Regions", x3, headerStyle = header_st)

  setColWidths(wb, sheet = "World_summary" , cols = 1:ncol(x1), widths = "auto")
  setColWidths(wb, sheet = "Country_details" , cols = 1:ncol(x2), widths = "auto")
  setColWidths(wb, sheet = "Regions" , cols = 1:ncol(x3), widths = "auto")

  style_comma <- createStyle(numFmt = "COMMA")

  for (y in 6:length(x1)) {
    addStyle(wb, "World_summary", cols = y, rows = 1:nrow(x1)+1, style = style_comma, gridExpand = TRUE, stack = TRUE)
  }

  for (y in c((6:length(x2)) [6:length(x2)%%3 == 0])) {
    addStyle(wb, "Country_details", cols = y, rows = 1:nrow(x2)+1, style = style_comma, gridExpand = TRUE, stack = TRUE)
  }

  for (y in 6:length(x3)) {
    addStyle(wb, "Regions", cols = y, rows = 1:nrow(x3)+1, style = style_comma, gridExpand = TRUE, stack = TRUE)
  }

  saveWorkbook(wb, tmp_file_commoditytables[i], overwrite = TRUE)

}

tmp_file_world <- file.path(TMP_DIR, paste0(min_year, "_", max_year, "_trade_commodity_tables.xlsx"))

world2 <- world[`Trade Dimension` %!in% c("Import UV [$/t]", "Export UV [$/t]", "Import UV [$/head]",
                                          "Export UV [$/head]", "Import UV [$/1000 head]", "Export UV [$/1000 head]"),]

z <- c(unique(world2$`Trade Dimension`[grepl("Import_Quantity", world2$`Trade Dimension`)]), unique(world2$`Trade Dimension`[grepl("Export_Quantity", world2$`Trade Dimension`)]), "Import - Export", "[(Import/Export) - 1] in %", "[Import_growth] in %",
       "[Export_growth] in % ", "Status", "Import Value [1000 $]", "Export Value [1000 $]")


world2 <- world2 %>% slice(order(factor(`Trade Dimension`, levels = z)))
world2 <-  world2[order(world2$`Commodity CPC Code`),, drop=FALSE]

N = 1
after_rows = 9

world2<- do.call(rbind, lapply(split(world2, ceiling(1:NROW(world2)/after_rows)),
                               function(a) rbind(a, replace(a[1:N,], TRUE, ""))))

world2 <- as.data.table(world2)
world2[, Country:= NULL]

year_cols <- c(names(world2)[names(world2) %!in% c("Country Group", "Commodity name", "Commodity CPC Code", "Trade Dimension")])
world2[,(year_cols):= lapply(.SD, as.numeric), .SDcols = year_cols]

wb <- createWorkbook("Creator of workbook2")
addWorksheet(wb, sheetName = "World_trade_tables")

header_st <- createStyle(textDecoration = "Bold", border = c('Top', 'Bottom', 'Left', 'Right'), borderColour = 'black')
style_comma <- createStyle(numFmt = "COMMA")
first_fill <- createStyle(fgFill = "#F34B43")

writeData(wb, 'World_trade_tables', world2, headerStyle = header_st)

setColWidths(wb, sheet = "World_trade_tables" , cols = 1:ncol(world2), widths = "auto")


for (i in 5:ncol(world2)){
  addStyle(wb, "World_trade_tables", cols = i, rows = 1 + c(na.omit((1:nrow(world2))[world2[[i]] == 0 & world2$`Trade Dimension` == 'Status'])),
           style = first_fill, gridExpand = TRUE, stack = TRUE)
}

for (i in 5:ncol(world2)) {
  addStyle(wb, "World_trade_tables", cols = i, rows = 1:nrow(world2), style = style_comma, gridExpand = TRUE, stack = TRUE)
}

saveWorkbook(wb, tmp_file_world, overwrite = TRUE)
# saveWorkbook(wb,file = "C:/Users/aydan/Desktop/world.xlsx", overwrite = TRUE)

# files2zip <- dir("C:/Users/aydan/Desktop/items_nonlivestock/", full.names = TRUE)
# zipped <- zip(zipfile = 'C:/Users/aydan/Desktop/testzip', files = files2zip)


bodyCommodityTables = paste("Plugin completed.")

send_mail(from = "no-reply@fao.org", subject = "Commodity tables", body = c(bodyCommodityTables, tmp_file_commoditytables), remove = TRUE)
# send_mail(from = "no-reply@fao.org", subject = "Commodity tables", body = c(bodyCommodityTables, tmp_file_commoditytables[21:length(tmp_file_commoditytables)]), remove = TRUE)
send_mail(from = "no-reply@fao.org", subject = "World", body = c(bodyCommodityTables, tmp_file_world), remove = TRUE)

print('Plug-in Completed')
