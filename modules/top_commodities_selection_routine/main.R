##'
##' **Author: Aydan Selek**
##'
##' **Description:**
##'
##' This module is designed to send the commodities ranked by quantity for non-reporting countries which needs to be re-filled by the country analyst.
##'
##'
##' **Inputs:**
##'
##' * total trade data
##'
##' **Flag assignment:**
##'
##' None

message("TRADE: Top commodities selection routine starts...")

## Load the libraries
library(faosws)
library(data.table)
library(faoswsUtil)
library(sendmailR)
library(openxlsx)
library(futile.logger)
suppressPackageStartupMessages(library(dplyr, warn.conflicts = FALSE))


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

sapply(dir("R", full.names = TRUE), source)

R_SWS_SHARE_PATH <- Sys.getenv("R_SWS_SHARE_PATH")
dev_sws_set_file <- "modules/top_commodities_selection_routine/sws.yml"
if (CheckDebug()) {
  set_sws_dev_settings(dev_sws_set_file)
}

`%!in%` = Negate(`%in%`)

year = as.numeric(swsContext.computationParams$year)

# the average will be calculated with the previous 5 years
interval <- (year-1):(year-5)

# data need to retrieved also including the current year
yearVals <- year:(year-5)

USER <- regmatches(
  swsContext.username,
  regexpr("(?<=/).+$", swsContext.username, perl = TRUE)
)

COUNTRY <- as.character(swsContext.datasets[[1]]@dimensions$geographicAreaM49@keys)

# Get data configuration and session
sessionKey = swsContext.datasets[[1]]

sessionCountries = getQueryKey("geographicAreaM49", sessionKey)

geoKeys = GetCodeList(domain = "trade", dataset = "total_trade_cpc_m49",
                      dimension = "geographicAreaM49")[type == "country", code]


# Select the countries based on the user countries
selectedGEOCode = sessionCountries

itemKeys = GetCodeList(domain = "trade", dataset = "total_trade_cpc_m49", "measuredItemCPC")
itemKeys = itemKeys[, code]

eleKeys <- GetCodeList(domain = "trade", dataset = "total_trade_cpc_m49", "measuredElementTrade")
eleKeys <- eleKeys[, code]

# Define geo dimension
geoDim = Dimension(name = "geographicAreaM49", keys = selectedGEOCode)

# Define element dimension
eleDim <- Dimension(name = "measuredElementTrade", keys = eleKeys)

# Define item dimension
itemDim <- Dimension(name = "measuredItemCPC", keys = itemKeys)

# Define time dimension
timeDim <- Dimension(name = "timePointYears", keys = as.character(yearVals))

# Define the key to pull Trade data
key = DatasetKey(domain = "trade", dataset = "total_trade_cpc_m49", dimensions = list(
  geographicAreaM49 = geoDim,
  measuredElementTrade = eleDim,
  measuredItemCPC = itemDim,
  timePointYears = timeDim
))


#### ACTUAL VALUES READ AND AGGREGATE FOR 'yearVals'###
# The RDS files are coming from TP selection plug-in. The data is in complete trade flow level.
# The actual values will be used to show which figures have been deleted by the TP selection plug-in.
# It is only to easy the job of country analyst.

actual_value_current = setDT(readRDS(paste0(R_SWS_SHARE_PATH, '/trade/validation_tool_files/tp_criterion/', year, '.rds')))
actual_value_1 = setDT(readRDS(paste0(R_SWS_SHARE_PATH,'/trade/validation_tool_files/tp_criterion/', (year-1), '.rds')))
actual_value_2 = setDT(readRDS(paste0(R_SWS_SHARE_PATH,'/trade/validation_tool_files/tp_criterion/', (year-2), '.rds')))
actual_value_3 = setDT(readRDS(paste0(R_SWS_SHARE_PATH,'/trade/validation_tool_files/tp_criterion/', (year-3), '.rds')))
actual_value_4 = setDT(readRDS(paste0(R_SWS_SHARE_PATH,'/trade/validation_tool_files/tp_criterion/', (year-4), '.rds')))

actual_value_current_value <- copy(actual_value_current)
setDT(actual_value_current_value)
actual_value_current_value <- actual_value_current_value[(measuredElementTrade == 5622 | measuredElementTrade == 5922) & geographicAreaM49Reporter == COUNTRY,
                                             .(Value = sum(Value)), by = .(timePointYears, geographicAreaM49Reporter,measuredItemCPC, measuredElementTrade)]
######################

actual_value_current <- actual_value_current[(measuredElementTrade == 5610 | measuredElementTrade == 5910) & geographicAreaM49Reporter == COUNTRY,
                                            .(Value = sum(Value)), by = .(timePointYears, geographicAreaM49Reporter,measuredItemCPC, measuredElementTrade)]

actual_value_1 <- actual_value_1[(measuredElementTrade == 5610 | measuredElementTrade == 5910) & geographicAreaM49Reporter == COUNTRY,
                                            .(Value = sum(Value)), by = .(timePointYears, geographicAreaM49Reporter,measuredItemCPC, measuredElementTrade)]

actual_value_2 <- actual_value_2[(measuredElementTrade == 5610 | measuredElementTrade == 5910) & geographicAreaM49Reporter == COUNTRY,
                                            .(Value = sum(Value)), by = .(timePointYears, geographicAreaM49Reporter,measuredItemCPC, measuredElementTrade)]
actual_value_3 <- actual_value_3[(measuredElementTrade == 5610 | measuredElementTrade == 5910) & geographicAreaM49Reporter == COUNTRY,
                                            .(Value = sum(Value)), by = .(timePointYears, geographicAreaM49Reporter,measuredItemCPC, measuredElementTrade)]
actual_value_4 <- actual_value_4[(measuredElementTrade == 5610 | measuredElementTrade == 5910) & geographicAreaM49Reporter == COUNTRY,
                                            .(Value = sum(Value)), by = .(timePointYears, geographicAreaM49Reporter,measuredItemCPC, measuredElementTrade)]

actual_value_total <- do.call("rbind", list(actual_value_current, actual_value_1, actual_value_2, actual_value_3, actual_value_4))

if (nrow(actual_value_total)>0){
  actual_value_total <- dcast.data.table(actual_value_total, geographicAreaM49Reporter + measuredItemCPC + measuredElementTrade
                                         ~ timePointYears, value.var = list('Value'))
}


setnames(actual_value_total, 'geographicAreaM49Reporter', 'geographicAreaM49')

####
non_reporting_countries <- ReadDatatable("ess_trade_apply_tp_criterion")

data = GetData(key, omitna = FALSE, normalized = FALSE)
data = normalise(data, areaVar = "geographicAreaM49",
               itemVar = "measuredItemCPC", elementVar = "measuredElementTrade",
               yearVar = "timePointYears", flagObsVar = "flagObservationStatus",
               flagMethodVar = "flagMethod", valueVar = "Value",
               removeNonExistingRecords = F)


trade <- nameData(domain = "trade", dataset = "total_trade_cpc_m49", data, except = "timePointYears")


trade1 <- trade[grepl("Quantity", measuredElementTrade_description),]
trade1 <- trade1[!(measuredElementTrade==5610 & stringr::str_sub(measuredItemCPC, 1, 3) == '021'),]
trade1 <- trade1[!(measuredElementTrade==5910 & stringr::str_sub(measuredItemCPC, 1, 3) == '021'),]
average <- trade1[timePointYears %in% interval, .(`5_year_average` = mean(Value, na.rm = TRUE)),
       by=.(geographicAreaM49, measuredElementTrade, measuredItemCPC)]

trade2 <- dcast.data.table(trade1, geographicAreaM49 + geographicAreaM49_description + measuredItemCPC +
                             measuredItemCPC_description + measuredElementTrade + measuredElementTrade_description
                           ~ timePointYears, value.var = list('Value'))

official_data <- trade1[, official:= ifelse(flagObservationStatus=='' & flagMethod=='s',TRUE, FALSE)]

official_data2 <- dcast.data.table(official_data, geographicAreaM49 + geographicAreaM49_description + measuredItemCPC +
                                     measuredItemCPC_description + measuredElementTrade + measuredElementTrade_description
                                   ~ timePointYears , value.var = list('official'))

trade3 <- merge(trade2, average, by = c('geographicAreaM49', 'measuredItemCPC',  'measuredElementTrade'))

trade_import <- trade3[grepl("Import", measuredElementTrade_description),][order(-`5_year_average`)]

# outList_1 <- trade_import[, head(.SD, 20), geographicAreaM49]
outList_1 <- trade_import[`5_year_average` > 100,]

trade_export <- trade3[grepl("Export", measuredElementTrade_description),][order(-`5_year_average`)]

# outList_2 <- trade_export[, head(.SD, 20), geographicAreaM49]
outList_2 <- trade_export[`5_year_average` > 100,]

outList_final <- rbind(outList_1, outList_2)

### Asssing the official figures ###
outList_to_official <- outList_final[,.(geographicAreaM49, geographicAreaM49_description, measuredItemCPC,
                                       measuredItemCPC_description, measuredElementTrade,
                                       measuredElementTrade_description)]
outList_to_official$id <- 1:nrow(outList_to_official)

official_data3 <- merge(outList_to_official, official_data2, by=c('geographicAreaM49', 'geographicAreaM49_description', 'measuredItemCPC',
                                                            'measuredItemCPC_description', 'measuredElementTrade',
                                                            'measuredElementTrade_description'), all.x=T, all.y=F)

official_data3 <- official_data3[order(official_data3$id), ]

official_data3[,id:=NULL]

### Assign the actual values ###
if (nrow(actual_value_total)>0){
  outList_to_actuals <- outList_final[,.(geographicAreaM49, geographicAreaM49_description, measuredItemCPC,
                                         measuredItemCPC_description, measuredElementTrade,
                                         measuredElementTrade_description)]
  outList_to_actuals$id <- 1:nrow(outList_to_actuals)

  actual_value_total$geographicAreaM49 <- as.character(actual_value_total$geographicAreaM49)
  actual_value_total$measuredElementTrade <- as.character(actual_value_total$measuredElementTrade)


  actual_value_total2 <- merge(outList_to_actuals, actual_value_total, by=c('geographicAreaM49', 'measuredItemCPC',
                                                                            'measuredElementTrade'), all.x=T, all.y=F)

  actual_value_total2 <- actual_value_total2[order(actual_value_total2$id), ]

  actual_value_total2[,id:=NULL]

  idvars = c("geographicAreaM49", "measuredItemCPC", "measuredElementTrade", "geographicAreaM49_description",
             "measuredItemCPC_description","measuredElementTrade_description")

  outList_final2 <- melt.data.table(outList_final, id.vars = idvars,
                                    measure.vars = c(names(outList_final)[names(outList_final) %!in% idvars]), variable.name = 'timePointYears', value.name = 'Value')

  actual_value_total2 <- melt.data.table(actual_value_total2, id.vars = idvars,
                                         measure.vars = c(names(actual_value_total2)[names(actual_value_total2) %!in% idvars]), variable.name = 'timePointYears', value.name = 'Value')

  outList_final3 <- merge(outList_final2, actual_value_total2, by = c(idvars, 'timePointYears'), all = TRUE )

  outList_final3 <- outList_final3[is.na(Value.x), Value.x:=Value.y]
  outList_final3 <- outList_final3[, Value.y := NULL]
  setnames(outList_final3, 'Value.x', 'Value')

  outList_final4 <- dcast.data.table(outList_final3, geographicAreaM49 + geographicAreaM49_description + measuredItemCPC +
                                       measuredItemCPC_description + measuredElementTrade + measuredElementTrade_description
                                     ~ timePointYears, value.var = list('Value'))

  ordering <- outList_final[,.(geographicAreaM49, geographicAreaM49_description, measuredItemCPC,
                               measuredItemCPC_description, measuredElementTrade,
                               measuredElementTrade_description)]
  ordering$id <- 1:nrow(ordering)

  outList_final4 <- merge(ordering, outList_final4, by=c('geographicAreaM49', 'geographicAreaM49_description', 'measuredItemCPC',
                                                         'measuredItemCPC_description', 'measuredElementTrade',
                                                         'measuredElementTrade_description'), all.x=T, all.y=F)

  outList_final4 <- outList_final4[order(outList_final4$id), ]

  outList_final4[,id:=NULL]

  ###


  #### ADD DELETED VALUE COLUMN #####
  setnames(actual_value_current_value, 'geographicAreaM49Reporter', 'geographicAreaM49')
  actual_value_current_value[, flow:= substr(measuredElementTrade, 1, 2)]
  actual_value_current_value[, measuredElementTrade:= NULL]
  if (nrow(actual_value_current_value)>0){
    actual_value_current_value[timePointYears==year, timePointYears_dollar:= paste0(year, '_Dollar_value')]
    actual_value_current_value[,timePointYears:= NULL]
    actual_value_current_value <- dcast.data.table(actual_value_current_value, geographicAreaM49 + measuredItemCPC + flow
                                                    ~ timePointYears_dollar, value.var = list('Value'))
  }



  outList_final5 <- outList_final4[, flow:= substr(measuredElementTrade, 1, 2)]

  if (nrow(actual_value_current_value)>0){
    outList_final6 <- merge(outList_final5, actual_value_current_value,
                            by = c('geographicAreaM49', 'measuredItemCPC', 'flow'), all.x = TRUE)
  } else {
    outList_final6 <- outList_final5
  }



  outList_final6 <- outList_final6[, flow:=NULL]


  ordering <- outList_final[,.(geographicAreaM49, geographicAreaM49_description, measuredItemCPC,
                               measuredItemCPC_description, measuredElementTrade,
                               measuredElementTrade_description)]
  ordering$id <- 1:nrow(ordering)

  outList_final7 <- merge(ordering, outList_final6, by=c('geographicAreaM49', 'geographicAreaM49_description', 'measuredItemCPC',
                                                         'measuredItemCPC_description', 'measuredElementTrade',
                                                         'measuredElementTrade_description'), all.x=T, all.y=F)

  outList_final7 <- outList_final7[order(outList_final7$id), ]

  outList_final7[,id:=NULL]

  outList_final5 <- outList_final7

} else {
  outList_final5 <- outList_final
}


name_of_country <- unique(outList_final5$geographicAreaM49_description)
# outList_final5 <- outList_final4[, measuredItemCPC := paste0("'", measuredItemCPC)]

# Create temporary location for the output
TMP_DIR <- file.path(tempdir())
if (!file.exists(TMP_DIR)) dir.create(TMP_DIR, recursive = TRUE)
tmp_file_tpselection <- file.path(TMP_DIR, paste0(name_of_country, "_main_commodities.xlsx"))


#### DESIGN THE EXCEL FILE #####

wb <- createWorkbook("Creator of workbook")
addWorksheet(wb, sheetName = "main_commodities")
writeData(wb, "main_commodities", outList_final5)

official <- createStyle(fontColour = "black", textDecoration = "bold")
# second_fill <- createStyle(fgFill = "orange")
first_fill <- createStyle(fgFill = "red")
# options("openxlsx.numFmt" = "0") # no decimal formating
# styleT <- createStyle(numFmt = "#,##0") # create thousands format
style_comma <- createStyle(numFmt = "COMMA")
# very_small <- createStyle(borderColour = "blue", borderStyle = "double", border = "TopBottomLeftRight")
very_small <- createStyle(fgFill = "yellow")
very_big <- createStyle(fgFill = "deepskyblue")
# for (i in nrow(outList_final5)) {
#    addStyle(wb, "main_commodities", cols = 13, rows = 1 + c((1:nrow(outList_final))[is.na(outList_final[[12]])]), style = first_fill, gridExpand = TRUE)
# }

for (i in c(7,8,9,10,11,12)) {
      addStyle(wb, "main_commodities", cols = i, rows = 1 + c(na.omit((1:nrow(outList_final))[outList_final[[i]]/ outList_final$`5_year_average` < 0.5])), style = very_small, gridExpand = TRUE, stack = TRUE)
}

for (i in c(7,8,9,10,11,12)) {
  addStyle(wb, "main_commodities", cols = i, rows = 1 + c(na.omit((1:nrow(outList_final))[outList_final[[i]]/ outList_final$`5_year_average` > 2])), style = very_big, gridExpand = TRUE, stack = TRUE)
}

#
# for (i in c(7,8,9,10,11)) {
#   addStyle(wb, "main_commodities", cols = i + 1, rows = 1 + c((1:nrow(outList_final))[is.na(outList_final[[i]])]), style = second_fill, gridExpand = TRUE)
# }

for (i in c(7,8,9,10,11,12)) {
  addStyle(wb, "main_commodities", cols = i, rows = 1 + c((1:nrow(outList_final))[is.na(outList_final[[i]])]), style = first_fill, gridExpand = TRUE, stack = TRUE)
}

for (i in c(7,8,9,10,11,12)) {
  addStyle(wb, "main_commodities", cols = i, rows = 1 + c(na.omit((1:nrow(outList_final))[official_data3[[i]]])), style = official, gridExpand = TRUE, stack = TRUE)
}

for (i in c(7,8,9,10,11,12,13,14)) {
  addStyle(wb, "main_commodities", cols = i, rows = 1:nrow(outList_final5)+1, style = style_comma, gridExpand = TRUE, stack = TRUE)
}

# addStyle(wb, "main_commodities", cols = c(names(outList_final5)[names(outList_final5) %!in% idvars]), rows = 1:nrow(outList_final5)+1, style = style_comma, gridExpand = TRUE, stack = TRUE)


saveWorkbook(wb, tmp_file_tpselection, overwrite = TRUE)
bodyTPSelection = paste("Plugin completed. The attached excel file contains a list of main commodities.
                        ######### Figures description #########
                        Red figures: Missing or deleted bad Tp values;
                        Yellow figures: Values minimum 50% less than the 5 year average;
                        Blue figures: Values minimum twice more than the 5-year average;
                        Bold figures: Official data.
                        ",
                    sep='\n')

send_mail(from = "no-reply@fao.org", subject = paste("Main Commodities of ", name_of_country), body = c(bodyTPSelection, tmp_file_tpselection), remove = TRUE)

print('Plug-in Completed. Please see your e-mail.')
