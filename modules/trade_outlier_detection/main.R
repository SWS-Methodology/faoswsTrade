##'
##' **Author: Carlo Del Bello**
##'
##' **Description:**
##'
##' This module is designed to identify outliers in total trade data
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
library(openxlsx)
suppressPackageStartupMessages(library(dplyr, warn.conflicts = FALSE))

if (CheckDebug()) {
  message("Not on server, so setting up environment...")

  library(faoswsModules)
  SETT <- ReadSettings("sws.yml")

  R_SWS_SHARE_PATH <- SETT[["share"]]
  ## Get SWS Parameters
  # SetClientFiles(dir = SETT[["certdir"]])
  GetTestEnvironment(
    baseUrl = SETT[["server"]],
    token = SETT[["token"]]
  )
}


# sendMailAttachment=function(fileToSend,name,textBody){
#   if(dim(fileToSend)[1]>0){
#     if(!CheckDebug()){
#       # Create the body of the message
#
#       FILETYPE = ".csv"
#       CONFIG <- faosws::GetDatasetConfig(swsContext.datasets[[1]]@domain, swsContext.datasets[[1]]@dataset)
#       sessionid <- ifelse(length(swsContext.datasets[[1]]@sessionId),
#                           swsContext.datasets[[1]]@sessionId,
#                           "core")
#
#       basename <- sprintf("%s_%s",
#                           name,
#                           sessionid)
#       basedir <- tempfile()
#       dir.create(basedir, recursive = TRUE)
#       destfile <- file.path(basedir, paste0(basename, FILETYPE))
#
#       # create the csv in a temporary foldes
#       write.csv(fileToSend, destfile, row.names = FALSE)
#       # define on exit strategy
#       on.exit(file.remove(destfile))
#       #zipfile <- paste0(destfile, ".zip")
#       #withCallingHandlers(zip(zipfile, destfile, flags = "-j9X"),
#       # warning = function(w){
#       # if(grepl("system call failed", w$message)){
#       #  stop("The system ran out of memory trying to zip up your data. Consider splitting your request into chunks")
#       # }
#       # })
#
#       #on.exit(file.remove(zipfile), add = TRUE)
#       body = textBody
#
#       sendmailR::sendmail(from = "sws@fao.org",
#                           to = swsContext.userEmail,
#                           subject = name,
#                           msg = list(strsplit(body,"\n")[[1]],
#                                      sendmailR::mime_part(destfile,
#                                                           name = paste0(basename, FILETYPE)
#                                      )
#                           )
#       )
#     }
#   }
# }


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



# Create temporary location for the output
TMP_DIR <- file.path(tempdir())
if (!file.exists(TMP_DIR)) dir.create(TMP_DIR, recursive = TRUE)

startYear = as.numeric(swsContext.computationParams$startYear)
#startYear = as.numeric(2013)

endYear = as.numeric(swsContext.computationParams$endYear)
# window = as.numeric(swsContext.computationParams$window)
window = 5

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

# eleKeys <- GetCodeList(domain = "trade", dataset = "total_trade_cpc_m49", "measuredElementTrade")
# eleKeys <- eleKeys[, code]

eleDim <- c('5607', '5608', '5609', '5610', '5907', '5908', '5909', '5910', '5622', '5922',
            '5630', '5930', '5638', '5938', '5639', '5939', '5637', '5937') %>% Dimension(name = "measuredElementTrade", keys = .)

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

COUNTRY_NAME <- as.character(unique(trade$geographicAreaM49_description))
tmp_file_outlier <- file.path(TMP_DIR, paste0(COUNTRY_NAME, "_Outliers ", endYear, ".xlsx"))

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

outList <- trade[outlier == TRUE]

threshold_used <- unique(outList$threshold)

if (nrow(outList) > 0) {
  outList[, c("flow", "big_qty", "outlier", "threshold") := NULL]

  wb <- createWorkbook("Creator of workbook")
  addWorksheet(wb, sheetName = "Outliers")
  writeData(wb, "Outliers", outList)
  saveWorkbook(wb, tmp_file_outlier, overwrite = TRUE)
  # outList[, measuredItemCPC := paste0("'", measuredItemCPC)]

  bodyOutliers <- paste0("The Email contains a list of trade outliers based on Unit Value. The quantity threshold used for this country is: ", threshold_used)

  # sendMailAttachment(outList, paste0(COUNTRY_NAME, '_Outliers ', endYear), bodyOutliers)
  send_mail(from = "no-reply@fao.org", subject = paste0(COUNTRY_NAME, "_Outliers ", endYear), body = c(bodyOutliers, tmp_file_outlier), remove = TRUE)

  print("Outliers found, please check email.")
} else {

  print("No outliers found.")
}
