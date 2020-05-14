##'
##' **Author: Aydan Selek**
##'
##' **Description:**
##'
##' This plug-in is made for last check of trade data by the trade officers.
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
#   GetTestEnvironment(baseUrl = 'https://hqlqasws1.hq.un.fao.org:8181/sws', token = '343d015a-1476-43fa-a729-3e3ca47f945a')
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
tmp_file_tpselection <- file.path(TMP_DIR, paste0("trade_data_last_check_", COUNTRY, ".xlsx"))


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


# non_reporting_countries <- ReadDatatable("ess_trade_apply_tp_criterion")

# stopifnot(sessionCountries %in% non_reporting_countries$area)

data = GetData(key, omitna = FALSE, normalized = FALSE)
data = normalise(data, areaVar = "geographicAreaM49",
                 itemVar = "measuredItemCPC", elementVar = "measuredElementTrade",
                 yearVar = "timePointYears", flagObsVar = "flagObservationStatus",
                 flagMethodVar = "flagMethod", valueVar = "Value",
                 removeNonExistingRecords = F)


trade <- nameData(domain = "trade", dataset = "total_trade_cpc_m49", data, except = "timePointYears")


trade_qty <- trade[grepl("Quantity", measuredElementTrade_description),]

# In the final output, we would like to receive only qty in head, and qty in 1000 head for livestock items, NOT TO RECEIVE their respective quantity in tons
big_animals <- unique(trade_qty[measuredElementTrade %in% c(5608,5908), measuredItemCPC_description]) # select the animals in heads
small_animals <- unique(trade_qty[measuredElementTrade %in% c(5609,5909), measuredItemCPC_description]) # select the animel in 1000 heads
trade_in_heads <- trade_qty[measuredItemCPC_description %in% big_animals & measuredElementTrade %in% c(5608,5908),]
trade_in_1000heads <- trade_qty[measuredItemCPC_description %in% small_animals & measuredElementTrade %in% c(5609,5909),]
trade_qty_2 <- trade_qty[measuredItemCPC_description %!in% big_animals & measuredItemCPC_description %!in% small_animals,]

trade1 <- do.call('rbind', list(trade_qty_2, trade_in_heads, trade_in_1000heads))


average <- trade1[timePointYears %in% interval, .(`5_year_average` = mean(Value, na.rm = TRUE)),
                  by=.(geographicAreaM49, measuredElementTrade, measuredItemCPC)]

trade2 <- dcast.data.table(trade1, geographicAreaM49 + geographicAreaM49_description + measuredItemCPC +
                             measuredItemCPC_description + measuredElementTrade + measuredElementTrade_description
                           ~ timePointYears, value.var = list('Value'))

official_data <- trade1[, official:= ifelse(flagObservationStatus=='' & flagMethod=='s',TRUE, FALSE)]

official_data2 <- dcast.data.table(official_data, geographicAreaM49 + geographicAreaM49_description + measuredItemCPC +
                                     measuredItemCPC_description + measuredElementTrade + measuredElementTrade_description
                                   ~ timePointYears , value.var = list('official'))

trade3 <- merge(trade2, average, by = c('geographicAreaM49', 'measuredElementTrade', 'measuredItemCPC'))

trade_import <- trade3[grepl("Import", measuredElementTrade_description),][order(-`5_year_average`)]

# outList_1 <- trade_import[, head(.SD, numberOfItem), geographicAreaM49]
outList_1 <- trade_import[`5_year_average` > 100,]

trade_export <- trade3[grepl("Export", measuredElementTrade_description),][order(-`5_year_average`)]

# outList_2 <- trade_export[, head(.SD, numberOfItem), geographicAreaM49]
outList_2 <- trade_export[`5_year_average` > 100,]

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

#### DESIGN THE EXCEL FILE #####

wb <- createWorkbook("Creator of workbook")
addWorksheet(wb, sheetName = "trade_data_last_check")
writeData(wb, "trade_data_last_check", outList_final)

official <- createStyle(fontColour = "black", textDecoration = "bold")
# second_fill <- createStyle(fgFill = "orange")
first_fill <- createStyle(fgFill = "red")
very_small <- createStyle(fgFill = "yellow")
# very_small <- createStyle(borderColour = "blue", borderStyle = "double", border = "TopBottomLeftRight")
style_comma <- createStyle(numFmt = "COMMA")


# for (i in nrow(outList_final)) {
#   addStyle(wb, "trade_data_last_check", cols = 12, rows = 1 + c((1:nrow(outList_final))[is.na(outList_final[[12]])]), style = first_fill, gridExpand = TRUE)
# }

for (i in c(7,8,9,10,11,12)) {
  addStyle(wb, "trade_data_last_check", cols = i, rows = 1 + c((1:nrow(outList_final))[is.na(outList_final[[i]])]), style = first_fill, gridExpand = TRUE, stack = TRUE)
}

for (i in c(7,8,9,10,11,12)) {
  addStyle(wb, "trade_data_last_check", cols = i, rows = 1 + c(na.omit((1:nrow(outList_final))[outList_final[[i]]/ outList_final$`5_year_average` < 0.5])), style = very_small, gridExpand = TRUE, stack = TRUE)
}

# for (i in c(7,8,9,10,11)) {
#   addStyle(wb, "trade_data_last_check", cols = i, rows = 1 + c((1:nrow(outList_final))[is.na(outList_final[[i]])]), style = second_fill, gridExpand = TRUE)
# }

for (i in c(7,8,9,10,11,12)) {
  addStyle(wb, "trade_data_last_check", cols = i, rows = 1 + c(na.omit((1:nrow(outList_final))[official_data3[[i]]])), style = official, gridExpand = TRUE, stack = TRUE)
}

for (i in c(7,8,9,10,11,12,13)) {
  addStyle(wb, "trade_data_last_check", cols = i, rows = 1:nrow(outList_final)+1, style = style_comma, gridExpand = TRUE, stack = TRUE)
}

saveWorkbook(wb, tmp_file_tpselection, overwrite = TRUE)

bodyLastCheck = paste("Plugin completed. The attached excel file contains all import and export quantities, sorted by 5 years average.
                        ######### Figures description #########
                        Red figures: Missing values;
                        Yellow figures: Values minimum 50% less than the 5 year average;
                        Bold figures: Official data.
                        ",
                      sep='\n')

send_mail(from = "no-reply@fao.org", subject = "trade_data_last_check", body = c(bodyLastCheck, tmp_file_tpselection), remove = TRUE)

print('Plug-in Completed')
