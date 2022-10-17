##'
##' **Author: Aydan Selek**
##'
##' **Description:**
##'
##' This plugin helps analysts to calculate mirror trade statistics, in terms of quantity and dollar value.
##'
##'
##' **Inputs:**
##'
##' * complete trade data
##'
##' **Flag assignment:**
##'
##' None

message("mirror data calcution plugin")

##+ setup, include=FALSE
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

if (CheckDebug()) {
  library(faoswsModules)
  SETTINGS = ReadSettings("modules/calculate_mirror_quantity_value/sws.yml")
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

USER <- regmatches(
  swsContext.username,
  regexpr("(?<=/).+$", swsContext.username, perl = TRUE)
)

COUNTRY <- as.character(swsContext.datasets[[1]]@dimensions$geographicAreaM49Partner@keys)

# Create temporary location for the output
TMP_DIR <- file.path(tempdir())
if (!file.exists(TMP_DIR)) dir.create(TMP_DIR, recursive = TRUE)
tmp_file_miror <- file.path(TMP_DIR, paste0("calculate_mirror_quantity_value", COUNTRY, ".xlsx"))

##' - `year`: year for processing.
min_year <- as.integer(swsContext.computationParams$min_year)
max_year <- as.integer(swsContext.computationParams$max_year)

if (sum(min_year, max_year)==min_year) {
  max_year=min_year
}

# stopifnot(max_year >= min_year)

##' 1. `5607`: Import Quantity (number)
##' 1. `5608`: Import Quantity (heads)
##' 1. `5609`: Import Quantity (1000 heads)
##' 1. `5610`: Import Quantity (tonnes)
##' 1. `5907`: Export Quantity (number)
##' 1. `5908`: Export Quantity (heads)
##' 1. `5909`: Export Quantity (1000 heads)
##' 1. `5910`: Export Quantity (tonnes)
##' 1. `5622`: Imports (1,000 US$)
##' 1. `5922`: Exports (1,000 US$)

# Take session key
sessionKey = swsContext.datasets[[1]]
# Select session Reporter
sessionCountries = getQueryKey("geographicAreaM49Reporter", sessionKey)
# Select session Partners
sessionCountries_par = getQueryKey("geographicAreaM49Partner", sessionKey)

# Select session item
sessionItem = getQueryKey("measuredItemCPC", sessionKey)
# Define the key to pull trade data
key <-
  DatasetKey(
    domain = "trade",
    dataset = "completed_tf_cpc_m49",
    dimensions =
      list(
        geographicAreaM49Reporter = Dimension("geographicAreaM49Reporter", sessionCountries),
        geographicAreaM49Partner  = Dimension("geographicAreaM49Partner",  sessionCountries_par),
        measuredElementTrade      = Dimension("measuredElementTrade",      sessionKey@dimensions[["measuredElementTrade"]]@keys),
        measuredItemCPC           = Dimension("measuredItemCPC",           sessionItem),
        timePointYears            = Dimension("timePointYears",            as.character(min_year:max_year))
      )
  )

completetrade <- GetData(key)

stopifnot(nrow(completetrade) > 0)

setnames(completetrade, "geographicAreaM49Reporter", "geographicAreaM49")

completetrade2 <- copy(completetrade)

message("calculate mirror")

# Do not take into consideration the T flag for Quantity and Dollar Value.
completetrade2_dcasted <- dcast.data.table(completetrade2, geographicAreaM49 + geographicAreaM49Partner +  measuredItemCPC +
                                           timePointYears ~ measuredElementTrade, value.var = c('Value', 'flagObservationStatus', 'flagMethod'))


completetrade2_dcasted <- completetrade2_dcasted[!apply(completetrade2_dcasted, 1, function(r) any(r %in% c("T"))),]

# MELT the 3 variables over the unknown column anmes and then merge all....

cols_val <- c(names(completetrade2_dcasted)[grepl('Value_', names(completetrade2_dcasted))])

cols_flagObs <- c(names(completetrade2_dcasted)[grepl('flagObservationStatus_', names(completetrade2_dcasted))])

cols_flagMeth <- c(names(completetrade2_dcasted)[grepl('flagMethod_', names(completetrade2_dcasted))])

completetrade2_melted_1 <- melt.data.table(completetrade2_dcasted, id.vars = c("geographicAreaM49", "geographicAreaM49Partner", "measuredItemCPC", "timePointYears"),
                                         measure.vars = cols_val, variable.name = c('measuredElementTrade'), value.name = c('Value'))
completetrade2_melted_1[, measuredElementTrade:= gsub("Value_", "\\1", measuredElementTrade)]


completetrade2_melted_2 <- melt.data.table(completetrade2_dcasted, id.vars = c("geographicAreaM49", "geographicAreaM49Partner", "measuredItemCPC", "timePointYears"),
                                           measure.vars = c(cols_flagObs), variable.name = c('measuredElementTrade'), value.name = c('flagObservationStatus'))
completetrade2_melted_2[, measuredElementTrade:= gsub("flagObservationStatus_", "\\1", measuredElementTrade)]

completetrade2_melted_3 <- melt.data.table(completetrade2_dcasted, id.vars = c("geographicAreaM49", "geographicAreaM49Partner", "measuredItemCPC", "timePointYears"),
                                           measure.vars = cols_flagMeth, variable.name = c('measuredElementTrade'), value.name = c('flagMethod'))
completetrade2_melted_3[, measuredElementTrade:= gsub("flagMethod_", "\\1", measuredElementTrade)]

merge_all <- merge(completetrade2_melted_1, completetrade2_melted_2, by = c("geographicAreaM49", "geographicAreaM49Partner", "measuredItemCPC", "timePointYears", 'measuredElementTrade'))
merge_all <- merge(merge_all, completetrade2_melted_3, by = c("geographicAreaM49", "geographicAreaM49Partner", "measuredItemCPC", "timePointYears", 'measuredElementTrade'))


## Calculate mirror quantity ##
quantity <- merge_all[measuredElementTrade %in% c('5610', '5608', '5609', '5607', '5910', '5908', '5909', '5907')]
# mirrored_quantity <- sum(quantity$Value)
mirrored_quantity <- list()
for (i in 1:length(min_year:max_year)){
  year_i <- sort(unique(quantity$timePointYears))[i]
  mirrored_quantity[[i]] <- quantity[timePointYears==year_i, sum(Value)]
}


## Calculate mirror dolar value ##
value <- merge_all[measuredElementTrade %in% c('5622', '5922')]
# mirrored_value <- sum(value$Value)
mirrored_value <- list()
for (i in 1:length(min_year:max_year)){
  year_i <- sort(unique(value$timePointYears))[i]
  mirrored_value[[i]] <- value[timePointYears==year_i, sum(Value)]
}


Years <- c(min_year:max_year)
Quantity <- do.call(rbind, mirrored_quantity)
Value <- do.call(rbind, mirrored_value)
all <- data.frame(Years, Quantity, Value)

bodytosend <- list()
for (i in 1:length(min_year:max_year)){
  year_i <- sort(unique(value$timePointYears))[i]
  bodytosend <- c(bodytosend, paste0("Mirrored quantity of ", year_i, ' : ', round(mirrored_quantity[[i]], 4),
                       " Mirrored value of ", year_i, ' : ', round(mirrored_value[[i]], 4)))
}
bodytosend <- paste(bodytosend, collapse = "_______________ " )


if (min_year == max_year){
  print(paste0("Mirrored quantity is: ", mirrored_quantity, " Mirrored value is: ", mirrored_value))

} else {


  wb <- createWorkbook("Creator of workbook")
  addWorksheet(wb, sheetName = "mirror_quanity_value")
  writeData(wb, "mirror_quanity_value", all)

  # style_comma <- createStyle(numFmt = "COMMA")

  saveWorkbook(wb, tmp_file_miror, overwrite = TRUE)

  bodyMirror = paste("The dollar values below indicates only the pure total. Please do not forget to add CIF/FOB (+/-12%) effect, in case you will apply it.
                 #### RESULTS #### ",
                     bodytosend,
                     sep='\n')

  send_mail(from = "no-reply@fao.org", subject = paste0("Calculated Mirror Statistic for ", sessionCountries_par),
            body = c(bodyMirror, tmp_file_miror), remove = TRUE)

}



