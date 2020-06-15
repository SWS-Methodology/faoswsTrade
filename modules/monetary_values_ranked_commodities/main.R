##'
##' **Author: Aydan Selek**
##'
##' **Description:**
##'
##' This plug-in is made for monetary checks for the validation of Merchandise trade.
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
library(faosws)
library(data.table)
library(faoswsUtil)
library(sendmailR)
library(openxlsx)

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
              return(sendmailR:::.file_attachment(x, basename(x), type = file_type))
            }

            if (remove) {
              unlink(x)
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


# ## set up for the test environment and parameters
# R_SWS_SHARE_PATH = Sys.getenv("R_SWS_SHARE_PATH")

# if (CheckDebug()) {
#   SetClientFiles(dir = "C:/Users/aydan/Desktop/qa")
#
#   GetTestEnvironment(baseUrl = 'https://hqlqasws1.hq.un.fao.org:8181/sws', token = 'eee3ba1a-dabf-4771-815d-f4e47215561a')
# }


if(CheckDebug()){
  message("Not on server, so setting up environment...")

  library(faoswsModules)
  SETT <- ReadSettings("C:/Users/Selek/Dropbox/1-FAO-DROPBOX/faosws_Trade/modules/trade_data_last_check/sws.yml")

  R_SWS_SHARE_PATH <- SETT[["share"]]
  ## Get SWS Parameters
  SetClientFiles(dir = SETT[["certdir"]])
  GetTestEnvironment(
    baseUrl = SETT[["server"]],
    token = SETT[["token"]]
  )
}

year = as.numeric(swsContext.computationParams$year)
# year <- as.numeric(2018)

# numberOfItem = as.numeric(swsContext.computationParams$number_of_item_to_validate)
# numberOfItem = 10

# the average will be calculated with the previous 5 years
interval <- (year-1):(year-5)

# data need to retrieved also including the current year
yearVals <- year:(year-5)

USER <- regmatches(
  swsContext.username,
  regexpr("(?<=/).+$", swsContext.username, perl = TRUE)
)

COUNTRY <- as.character(swsContext.datasets[[1]]@dimensions$geographicAreaM49@keys)

# Create temporary location for the output
TMP_DIR <- file.path(tempdir())
if (!file.exists(TMP_DIR)) dir.create(TMP_DIR, recursive = TRUE)
tmp_file_tpselection <- file.path(TMP_DIR, paste0("monetary_values_ranked_commodities_", COUNTRY, ".xlsx"))


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

#########################################
#####   Pull data from total trade  #####
#########################################

message("TradeOUT: Pulling trade Data")

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


data = GetData(key, omitna = FALSE, normalized = FALSE)
data = normalise(data, areaVar = "geographicAreaM49",
                 itemVar = "measuredItemCPC", elementVar = "measuredElementTrade",
                 yearVar = "timePointYears", flagObsVar = "flagObservationStatus",
                 flagMethodVar = "flagMethod", valueVar = "Value",
                 removeNonExistingRecords = F)


trade <- nameData(domain = "trade", dataset = "total_trade_cpc_m49", data, except = "timePointYears")

trade_mnt <- trade[measuredElementTrade %in% c(5622,5922),]
# trade_qty <- trade[grepl("Quantity", measuredElementTrade_description),]


###### Calculate the Share ######

share <- trade_mnt[measuredItemCPC %in% c('F1881', 'F1882'),]
share_dcast <- dcast.data.table(share, geographicAreaM49 + measuredElementTrade + timePointYears ~ measuredItemCPC, value.var = c('Value'))

share_dcast <- share_dcast[, share:= (F1882/F1881)*100]

share_melted <- melt.data.table(share_dcast, id.vars=c("geographicAreaM49","timePointYears","measuredElementTrade"), variable.name = "measuredItemCPC", value.name = "Value")

share_melted <- share_melted[measuredItemCPC=='share']

setnames(share_melted, 'measuredItemCPC', 'measuredItemCPC_description')

share_last <- dcast.data.table(share_melted, geographicAreaM49+ measuredItemCPC_description+ measuredElementTrade~ timePointYears, value.var = c('Value'))

share_last[measuredItemCPC_description=='share', measuredItemCPC_description:='SHARE of Agriculture to Merchandise (%)']

###########

average <- trade_mnt[timePointYears %in% interval, .(`5_year_average` = mean(Value, na.rm = TRUE)),
                  by=.(geographicAreaM49, measuredElementTrade, measuredItemCPC)]

trade2 <- dcast.data.table(trade_mnt, geographicAreaM49 + geographicAreaM49_description + measuredItemCPC +
                             measuredItemCPC_description + measuredElementTrade + measuredElementTrade_description
                           ~ timePointYears, value.var = list('Value'))

official_data <- trade_mnt[, official:= ifelse(flagObservationStatus=='' & flagMethod=='s',TRUE, FALSE)]

official_data2 <- dcast.data.table(official_data, geographicAreaM49 + geographicAreaM49_description + measuredItemCPC +
                                     measuredItemCPC_description + measuredElementTrade + measuredElementTrade_description
                                   ~ timePointYears , value.var = list('official'))

trade3 <- merge(trade2, average, by = c('geographicAreaM49', 'measuredElementTrade', 'measuredItemCPC'))

trade_import <- trade3[grepl("Import", measuredElementTrade_description),][order(-`5_year_average`)]

# outList_1 <- trade_import[, head(.SD, numberOfItem), geographicAreaM49]
outList_1 <- trade_import[`5_year_average` > 100,]

outList_1 <- rbind(outList_1[1:2,],share_last[measuredElementTrade=='5622',],outList_1[-(1:2),], fill=TRUE)


trade_export <- trade3[grepl("Export", measuredElementTrade_description),][order(-`5_year_average`)]

# outList_2 <- trade_export[, head(.SD, numberOfItem), geographicAreaM49]
outList_2 <- trade_export[`5_year_average` > 100,]
outList_2 <- rbind(outList_2[1:2,],share_last[measuredElementTrade=='5922',],outList_2[-(1:2),], fill=TRUE)


outList_final <- rbind(outList_1, outList_2)


outList_to_official <- outList_final[,.(geographicAreaM49, geographicAreaM49_description, measuredItemCPC,
                                        measuredItemCPC_description, measuredElementTrade,
                                        measuredElementTrade_description)]
outList_to_official$id <- 1:nrow(outList_to_official)

official_data3 <- merge(outList_to_official, official_data2, by=c('geographicAreaM49', 'geographicAreaM49_description', 'measuredItemCPC',
                                                                  'measuredItemCPC_description', 'measuredElementTrade',
                                                                  'measuredElementTrade_description'), all.x=T, all.y=F)

official_data3 <- official_data3[order(official_data3$id), ]

official_data3[,id:=NULL]

outList_final <- outList_final[, measuredItemCPC := paste0("'", measuredItemCPC)]
outList_final <- outList_final[is.na(`5_year_average`), `5_year_average`:=0]

setcolorder(outList_final, c('geographicAreaM49', 'geographicAreaM49_description', 'measuredItemCPC', 'measuredItemCPC_description' , 'measuredElementTrade', 'measuredElementTrade_description',
                             "2013", "2014", "2015", "2016", "2017", "2018", "5_year_average") )

#### DESIGN THE EXCEL FILE #####

wb <- createWorkbook("Creator of workbook")
addWorksheet(wb, sheetName = "Monetary_values")
writeData(wb, "Monetary_values", outList_final)

setColWidths(wb, sheet = 1, cols = c(1,2,3,5:13), widths = "auto")

official <- createStyle(fontColour = "black", textDecoration = "bold")
three_rows <- createStyle(fgFill = "#D6ECF2") # blue for 3 rows
very_small <- createStyle(fgFill = "#F6F16C") # small numbers
high_percentage <- createStyle(fgFill = "#F34B43") # high percentage
missing <- createStyle(fgFill = "#F39393") #missing
style_comma <- createStyle(numFmt = "COMMA")



for (i in 1:13) {
  addStyle(wb, "Monetary_values", cols = i, rows = 1 + c((1:nrow(outList_final))[outList_final$measuredItemCPC_description %in% c('Total Merchandise Trade', 'Agricult.Products,Total', 'SHARE of Agriculture to Merchandise (%)')]),
           style = three_rows, gridExpand = TRUE, stack = TRUE)
}


for (i in c(7,8,9,10,11,12)) {
  addStyle(wb, "Monetary_values", cols = i, rows = 1 + c(na.omit((1:nrow(outList_final))[outList_final[[7]] > 50 & outList_final$measuredItemCPC_description == 'SHARE of Agriculture to Merchandise (%)']))
           , style = high_percentage, gridExpand = TRUE, stack=TRUE)
}


for (i in c(7,8,9,10,11,12)) {
  addStyle(wb, "Monetary_values", cols = i, rows = 1 + c((1:nrow(outList_final))[is.na(outList_final[[i]])]), style = missing, gridExpand = TRUE, stack = TRUE)
}

for (i in c(7,8,9,10,11,12)) {
  addStyle(wb, "Monetary_values", cols = i, rows = 1 + c(na.omit((1:nrow(outList_final))[outList_final[[i]]/ outList_final$`5_year_average` < 0.5])), style = very_small, gridExpand = TRUE, stack = TRUE)
}


for (i in c(7,8,9,10,11,12)) {
  addStyle(wb, "Monetary_values", cols = i, rows = 1 + c(na.omit((1:nrow(outList_final))[official_data3[[i]]])), style = official, gridExpand = TRUE, stack = TRUE)
}

for (i in c(7,8,9,10,11,12,13)) {
  addStyle(wb, "Monetary_values", cols = i, rows = 1:nrow(outList_final)+1, style = style_comma, gridExpand = TRUE, stack = TRUE)
}

saveWorkbook(wb, tmp_file_tpselection, overwrite = TRUE)
# saveWorkbook(wb, file = "C:/Users/aydan/Desktop/dene9.xlsx", overwrite = TRUE)

bodyLastCheck = paste("Plugin completed. The attached excel file contains all import and export monetary values, sorted by 5 years average.
                        ######### Figures description #########
                        Red figures: SHARE of Agriculture to Merchandise is over 50%;
                        Yellow figures: Values minimum 50% less than the 5 year average;
                        Pink figures: Missing value;
                        Bold figures: Official data.
                        ",
                      sep='\n')

send_mail(from = "no-reply@fao.org", subject = "Monetary values ranked commodities", body = c(bodyLastCheck, tmp_file_tpselection), remove = TRUE)

print('Plug-in Completed')
