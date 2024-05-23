##'
##' **Author: Aydan Selek**
##'
##' **Description:**
##'
##' This plugin is for removing the protect flags of a country.
##'
##'
##' **Inputs:**
##'
##' * total trade data
##'
##' **Flag assignment:**
##'
##' Flag cancel

message("cancel protected flags")

library(data.table)
library(faoswsTrade)
library(faosws)
library(stringr)
library(scales)
library(faoswsUtil)
library(openxlsx)
library(faoswsFlag)
library(tidyr)
library(dplyr, warn.conflicts = FALSE)

if (CheckDebug()) {
  library(faoswsModules)
  SETTINGS = ReadSettings("modules/cancel_protected_flag/sws.yml")
  ## Define where your certificates are stored
  faosws::SetClientFiles(SETTINGS[["certdir"]])
  ## Get session information from SWS. Token must be obtained from web interface
  GetTestEnvironment(baseUrl = SETTINGS[["server"]],
                     token = SETTINGS[["token"]])
}

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


COUNTRY <- as.character(swsContext.datasets[[1]]@dimensions$geographicAreaM49@keys)
TMP_DIR <- file.path(tempdir())
if (!file.exists(TMP_DIR)) dir.create(TMP_DIR, recursive = TRUE)
tmp_file_cancelflag <- file.path(TMP_DIR, paste0("trade_cancel_flag_", COUNTRY, ".xlsx"))


min_year = as.numeric(swsContext.computationParams$min_year)
max_year = as.numeric(swsContext.computationParams$max_year)

# selection = as.character(swsContext.computationParams$selection)


years = min_year:max_year

# Get data configuration and session
sessionKey = swsContext.datasets[[1]]

sessionCountries = getQueryKey("geographicAreaM49", sessionKey)

geoKeys = GetCodeList(domain = "trade", dataset = "total_trade_cpc_m49",
                      dimension = "geographicAreaM49")[type == "country", code]

selectedGEOCode = sessionCountries

itemKeys = GetCodeList(domain = "trade", dataset = "total_trade_cpc_m49", "measuredItemCPC")
itemKeys = itemKeys[, code]

# eleKeys <- GetCodeList(domain = "trade", dataset = "total_trade_cpc_m49", "measuredElementTrade")
# eleKeys <- eleKeys[, code]

# Define geo dimension
geoDim = Dimension(name = "geographicAreaM49", keys = selectedGEOCode)

# Define element dimension
eleDim <- c('5607', '5608', '5609', '5610', '5907', '5908', '5909', '5910', '5622', '5922',
            '5630', '5930', '5638', '5938', '5639', '5939', '5637', '5937') %>% Dimension(name = "measuredElementTrade", keys = .)

# Define item dimension
itemDim <- Dimension(name = "measuredItemCPC", keys = itemKeys)

# Define time dimension
timeDim <- Dimension(name = "timePointYears", keys = as.character(years))

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



data_to_delete <- trade[(flagObservationStatus=='T' & flagMethod=='p')  |  # Legacy flag
                        (flagObservationStatus=='X' & flagMethod=='p')  |
                        (flagObservationStatus=='E' & flagMethod=='f')  |
                        (flagObservationStatus==''  & flagMethod=='p')  |  # Legacy flag
                        (flagObservationStatus==''  & flagMethod=='q')  |  # Legacy flag
                        (flagObservationStatus=='A' & flagMethod=='p')  |
                        (flagObservationStatus=='A' & flagMethod=='q'),]

data_to_delete <- data_to_delete[,Value:=NA]
data_to_delete <- data_to_delete[,flagObservationStatus:=NA]
data_to_delete <- data_to_delete[,flagMethod:=NA]


wb <- createWorkbook("Creator of workbook")
addWorksheet(wb, sheetName = "trade_cancel_flag")
writeData(wb, "trade_cancel_flag", data_to_delete)

saveWorkbook(wb, tmp_file_cancelflag, overwrite = TRUE)

bodyCancelFlag = paste("Plugin completed.")

send_mail(from = "no-reply@fao.org", subject = "trade_cancel_flag", body = c(bodyCancelFlag, tmp_file_cancelflag), remove = TRUE)


sessionKey = swsContext.datasets[[1]]
datasetConfig = GetDatasetConfig(domainCode = sessionKey@domain,
                                 datasetCode = sessionKey@dataset)


SaveData(domain = datasetConfig$domain,
         dataset = datasetConfig$dataset,
         data = data_to_delete, waitTimeout = 2000000)


print('Plugin completed.')


