##'
##'
##' **Author: Aydan Selek**
##'
##' **Description:**
##' It is an automatic component that creates all the six tables of the Pre-processing Report for Trade.
##' The Pre-processing reports -in same format- were being produced by the plug-in of CAETANO. Over the time
##' there have been some changes on countries' codes and names. This plug-in has been created from scratch and
##' it stands alone.
##' It reads the raw data from R-share folder and the previous version of reports from the datatbles in SWS,
##' saves the new versions of reports to R-share folder and to SWS as datatable. It also generates and saves a complete excel file of the reports.
##' The implemented methodology is in use since January 2020.
##' * REPORT 1: Reporters by year
##' * REPORT 2: Non-reporting countries
##' * REPORT 3: Number records by reporter/year
##' * REPORT 4: Import and export content check
##' * REPORT 5: Check qty and value included
##' * REPORT 6: Missing data by report
##'
##' **Inputs:**
##' * Raw trade data
##' * Previous version of reports
##'
##' **Flag assignment:**
##' None

suppressPackageStartupMessages(library(data.table))
library(faoswsTrade)
library(faosws)
# library(faoswsModules)
library(dplyr)
library(stringr)
library(openxlsx)
library(faoswsUtil)
library(sendmailR)
library(faoswsModules)

# ## set up for the test environment and parameters
R_SWS_SHARE_PATH = Sys.getenv("R_SWS_SHARE_PATH")

if(CheckDebug()){
  message("Not on server, so setting up environment...")

  # library(faoswsModules)
  SETT <- ReadSettings("modules/pre_processing_report/sws.yml")

  R_SWS_SHARE_PATH <- SETT[["share"]]
  ## Get SWS Parameters
  SetClientFiles(dir = SETT[["certdir"]])
  GetTestEnvironment(
    baseUrl = SETT[["server"]],
    token = SETT[["token"]]
  )
}

# if (CheckDebug()) {
#   SetClientFiles(dir = "C:/Users/Selek/Documents/certificates/production")
#
#   GetTestEnvironment(baseUrl = 'https://swsdev.aws.fao.org:8181', token = 'ba4f9eb4-c716-4f47-98be-a505748e487a')
# }


`%!in%` = Negate(`%in%`)
options(warn=-1)

date_of_run <- Sys.Date() # To be able to follow the excel file's version, we will use date of run

# Set-up the parameters
maxYearToProcess <- as.numeric(swsContext.computationParams$maxYearToProcess)
minYearToProcess <- as.numeric(swsContext.computationParams$minYearToProcess)
# maxYearToProcess <- 2017
# minYearToProcess <- 2016

if (any(length(minYearToProcess) == 0, length(maxYearToProcess) == 0)) {
  stop("Missing min or max year")
}

if (maxYearToProcess < minYearToProcess) {
  stop('Max year should be greater or equal than Min year')
}

year_of_report = (minYearToProcess : maxYearToProcess)

# Read the yearly basis raw trade data for reporting
datapath <- file.path(R_SWS_SHARE_PATH, '/trade/datatables')
# datapath <- file.path('D:/WINDOWS/trade/datatables')

# file_ce = paste0('ce_combinednomenclature_unlogged_', year_of_report, '.rds') # Europe countries
# file_ct = paste0('ct_tariffline_unlogged_', year_of_report, '.rds') # UNSD countries
#
# file_path_ce = paste0(datapath, file_ce)
# file_path_ct = paste0(datapath, file_ct)

# Read the previous version of the pre-processing reports and save new versions
initial <- file.path(R_SWS_SHARE_PATH, "/selek/trade/preprocessing-reports")
save    <- file.path(R_SWS_SHARE_PATH, "trade/pre_processing_report")
# initial <- file.path('E:/FAO DESKTOP/TRADE/pre-processing reports v0.3')
# save    <- file.path("C:/Users/aydan selek/Desktop/pre_processing_report")

# Create temporary location for the excel output
TMP_DIR <- file.path(tempdir())
if (!file.exists(TMP_DIR)) dir.create(TMP_DIR, recursive = TRUE)
tmp_file_PreProcessing <- file.path(TMP_DIR, paste0("PreProcessing_reports_", date_of_run, ".xlsx"))

##### READ PREVIOUS VERSIONS OF REPORTS (NOT FROM SHARED FOLDER ANYMORE BUT FROM SWS DATATABLE) ####

# report_old_1 <- readRDS(file = file.path(initial, paste0('report_1.rds')))
report_old_1 <- ReadDatatable('reporters_by_year_new_version')
report_old_1 <- select_if(report_old_1, function(x){any(!is.na(x))})
for (col in 1:ncol(report_old_1)){
  colnames(report_old_1)[col] <-  sub("year_", "", colnames(report_old_1)[col])
}
# report_old_2 <- readRDS(file = file.path(initial, paste0('report_2.rds')))
report_old_2 <- ReadDatatable("non_reporting_countries_new_version")
report_old_2 <- select_if(report_old_2, function(x){any(!is.na(x))})
for (col in 1:ncol(report_old_2)){
  colnames(report_old_2)[col] <-  sub("year_", "", colnames(report_old_2)[col])
}
# report_old_3 <- readRDS(file = file.path(initial, paste0('report_3.rds')))
report_old_3<- ReadDatatable("number_records_by_reporter_year_new_version")
# report_old_4 <- readRDS(file = file.path(initial, paste0('report_4.rds')))
report_old_4 <- ReadDatatable('import_and_export_content_check_new_version')
# report_old_5 <- readRDS(file = file.path(initial, paste0('report_5.rds')))
report_old_5 <- ReadDatatable("check_qty_and_value_included_new_version")
# report_old_6 <- readRDS(file = file.path(initial, paste0('report_6.rds')))
report_old_6 <- ReadDatatable("missing_data_by_report_new_version")

country_names <- GetCodeList("trade", "total_trade_cpc_m49", "geographicAreaM49")
stopifnot(nrow(country_names) > 0)

country_names <- country_names[, .(m49 = code, description)]

# use_new_data_format <- ReadDatatable('ess_trade_use_new_unsd_format')

################# FUNCTIONS ################

########## MAIL SEND FUNCTION #############

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

########## CONVERTING FUNCTIONS ###########

convert_geonom_to_faoandm49 <- function(data, reporter = "declarant") {
  geonom2fao <- as.data.table(copy(faoswsTrade::geonom2fao))

  orig_codes <- unique(data[[reporter]][grepl("^\\d+$", data[[reporter]])])
  num_codes <- as.numeric(orig_codes)

  tab <- data.table(geonom = orig_codes, code = num_codes)

  eu_countries <- geonom2fao[code %in% num_codes][, .(code, fao = faostat)]

  eu_countries[, m49 := faoswsUtil::fs2m49(as.character(fao))]

  res <- merge(eu_countries, tab, by = "code", all = TRUE)
  res[, code:=NULL]

  setnames(res, "geonom", "code")

  return(res[, .(code, m49, fao)])
}


convert_m49_to_faoandm49 <- function(data, reporter = "rep") {
  m49faomap <- as.data.table(copy(m49faomap))

  res <- copy(data)
  res <- unique(res[[reporter]])


  tab <- data.table(m49 = res)
  tab[, m49 := as.integer(m49)]

  tab <- merge(tab, m49faomap, by = "m49", all.x = TRUE)

  setnames(tab, "m49", "code") # Now 'code' is UNSD m49

  tab[, m49 := faoswsUtil::fs2m49(as.character(fao))] # And 'm49' is official

  return(tab[, .(code, m49, fao)])
}

########## REPORTING FUNCTION ###########
createReports<- function(year, names = country_names, report_number = 1){

  file_ce = paste0('/ce_combinednomenclature_unlogged_', year, '.rds')
  file_ct = paste0('/ct_tariffline_unlogged_', year, '.rds')

  file_path_ce = paste0(datapath, file_ce)
  file_path_ct = paste0(datapath, file_ct)

  data_ce = readRDS(file_path_ce)
  data_ct = readRDS(file_path_ct)

  # New UNSD data structure
  # if (year %in% unique(use_new_data_format$year)){
    file_ct_2 = paste0('/ct_tariffline_unlogged_', year, '_PATCH.rds')
    file_path_ct_2 = paste0(datapath, file_ct_2)
    data_ct_2 = readRDS(file_path_ct_2)
    data_ct_2_countries <- unique(data_ct_2$rep)
    data_ct <- data_ct[rep %!in% data_ct_2_countries, ]

    # data_ct <- data_ct[!(rep %in% use_new_data_format$area[use_new_data_format$year==year])]
    data_ct <- rbind(data_ct, data_ct_2, fill = TRUE)
  # }

  setDT(data_ce)
  setDT(data_ct)

  hs_chapters <- c("01","02", "03", "04", "05", "06","07", "08", "09", "10", "11", "12","13", "14", "15", "16",
                   "17", "18", "19", "20", "21", "22", "23", "24", "33", "35", "38", "40", "41", "43", "50", "51", "52", "53")

  data_ce <- data_ce[, chapter:= substr(product_nc, start=1,2)]
  data_ce <- data_ce[chapter %in% hs_chapters]
  data_ct <- data_ct[chapter %in% hs_chapters]

  data_ce <- data_ce[, remover:=ifelse(nchar(product_nc)==2, NA, product_nc)]
  data_ce <- na.omit(data_ce, cols="remover")
  data_ct <- data_ct[, remover:=ifelse(nchar(as.character(comm))==2, NA, comm)]
  data_ct <- na.omit(data_ct, cols="remover")

  eu_codes <- convert_geonom_to_faoandm49(data_ce)
  world_codes <- convert_m49_to_faoandm49(data_ct)
  com_codes <- world_codes[fao %!in% (eu_codes$fao)]

  # The column 'code' should contain the Geonom code for eurospean countries and the UNSD M49 codefor the rest
  com_codes <- com_codes[,colnames(eu_codes),with=FALSE]
  forreport <- rbind(eu_codes,com_codes[!eu_codes,on=c("fao", "m49")])
  forreport <- setDT(forreport)
  report <- merge(forreport, names, by='m49' )

  if (report_number == 1) {
    ## REPORT 1 ###
    reportersList1 <- unique(eu_codes[code %in% unique(data_ce[, declarant]), m49])
    reportersList2 <- unique(com_codes[code %in% unique(data_ct[, rep]), m49])
    reportersList <- unique(c(reportersList1, reportersList2))

    report <- report[, is_reporter:=ifelse(m49 %in% reportersList,'X',NA)]

    report <- report[, fao:=NULL]
    reportfinal <- report[, year:=year]
    #report <- report[, element:='report1']

  } else if (report_number == 2) {
    ## REPORT 2 ###
    EU_imports <- unique(eu_codes[code %in% unique(data_ce[flow=='1', declarant]), m49])
    EU_exports <- unique(eu_codes[code %in% unique(data_ce[flow=='2', declarant]), m49])

    Com_imports <- unique(com_codes[code %in% unique(data_ct[flow=='1'| flow=='4', rep]), m49])
    Com_exports <- unique(com_codes[code %in% unique(data_ct[flow=='2'| flow=='3', rep]), m49])

    imports_total <- unique(c(EU_imports, Com_imports))
    exports_total <- unique(c(EU_exports, Com_exports))


    report <- report[, is_reporter:=ifelse(m49 %!in% imports_total & m49 %!in% exports_total,9,
                                           ifelse(m49 %!in% imports_total & m49 %in% exports_total, 1,
                                                  ifelse(m49 %in% imports_total & m49 %!in% exports_total,2,NA)))]

    report <- report[, fao:=NULL]
    reportfinal <- report[, year:=year]

  } else if (report_number == 3) {
    ## REPORT 3 ###
    eurNumRec <- data_ce[,.(hs_min= min(nchar(product_nc)),
                            hs_max=max(nchar(product_nc)),
                            hs_n= length(unique(nchar(product_nc))), .N), by=.(declarant, flow)]
    setnames(eurNumRec, 'declarant', 'code')
    eurNumRec <- merge(eurNumRec, eu_codes, by='code', all.x = TRUE)
    eurNumRec <- merge(eurNumRec, report, by=c("fao","code", 'm49'), all.x = TRUE)
    eurNumRec <- eurNumRec[is.na(description)!= TRUE,]

    comNumRec <- data_ct[,.(hs_min= min(nchar(as.character(comm))),
                            hs_max=max(nchar(as.character(comm))),
                            hs_n= length(unique(nchar(as.character(comm)))), .N), by=.(rep, flow)]
    setnames(comNumRec, 'rep', 'code')
    comNumRec$code <- as.numeric(as.character(comNumRec[,code]))
    comNumRec <- merge(comNumRec, com_codes, by='code', all.y = TRUE)

    comNumRec$code <- as.character(comNumRec[,code])
    comNumRec <- merge(comNumRec, report, by=c("fao","code", 'm49'), all.x = TRUE)

    eurNumRec <- eurNumRec[,colnames(comNumRec),with=FALSE]
    report <- rbind(eurNumRec,comNumRec[!eurNumRec,on=c("description", "m49")])

    report <- report[, fao:=NULL]
    reportfinal <- report[, year:=year]

  } else if (report_number == 4) {
    ## REPORT 4 ###
    eurTrans <- data_ce[,.N, by=.(declarant, flow)]
    setnames(eurTrans, 'declarant', 'code')
    eurTrans <- merge(eurTrans, eu_codes, by='code', all.x = TRUE)
    eurTrans <- eurTrans[, N:=NULL]
    eurTrans <- merge(eurTrans, report, by=c("fao","m49","code"), all.x = TRUE)
    eurTrans <- eurTrans[is.na(description)!= TRUE,]

    comTrans <- data_ct[,.N, by=.(rep, flow)]
    setnames(comTrans, 'rep', 'code')
    comTrans$code <- as.numeric(as.character(comTrans[,code]))
    comTrans <- merge(comTrans, com_codes, by='code', all.y = TRUE)
    #comTrans <- comTrans[, rep_name:=NULL]
    comTrans <- comTrans[, N:=NULL]
    comTrans$code <- as.character(comTrans[,code])
    comTrans <- merge(comTrans, report, by=c("fao", "m49", "code"), all.x = TRUE)

    eurTrans <- eurTrans[,colnames(comTrans),with=FALSE]
    ans <- rbind(eurTrans,comTrans[!eurTrans,on=c("description", "m49")])

    ans1 <- ans[flow==1, .(code, m49, fao)]
    ans2 <- ans[flow==2, .(code, m49, fao)]
    ans3 <- ans[flow==3, .(code, m49, fao)]
    ans4 <- ans[flow==4, .(code, m49, fao)]
    # 1:imports, 2:exports, 3:re-exports, 4:re-imports
    report <- report[, exports:= ifelse(m49 %in% ans2$m49,'X',NA)]
    report <- report[, imports:= ifelse(m49 %in% ans1$m49,'X',NA)]
    report <- report[, re_exports:= ifelse(m49 %in% ans3$m49,'X',NA)]
    report <- report[, re_imports:= ifelse(m49 %in% ans4$m49,'X',NA)]

    report <- report[, year:=year]
    reportfinal <- report[, fao:=NULL]

  } else if (report_number == 5) {
    ## REPORT 5 ###
    eurQty = data_ce[, .(qty = as.character(ifelse((is.na(qty_ton) | qty_ton == 0) & (is.na(sup_quantity) | sup_quantity == 0),"X",NA)),
                         value = as.character(ifelse((is.na(value_1k_euro) | value_1k_euro == 0),"X",NA))), .(declarant, flow)]

    setnames(eurQty, 'declarant', 'code')
    eurQty <- merge(eurQty, eu_codes, by='code', all.x = TRUE)
    eurQty <- merge(eurQty, report, by=c("fao", "m49", "code"), all.x = TRUE)
    eurQty <- eurQty[is.na(description)!= TRUE,]

    comQty = data_ct[, .(qty = as.character(ifelse((is.na(weight) | weight == 0) & (is.na(qty) | qty == 0),"X",NA)),
                         value = as.character(ifelse((is.na(tvalue) | tvalue == 0),"X",NA))), .(rep, flow)]
    setnames(comQty, 'rep', 'code')
    comQty$code <- as.numeric(as.character(comQty[,code]))
    comQty <- merge(comQty, com_codes, by='code', all.y = TRUE)
    comQty$code <- as.character(comQty[,code])
    comQty <- merge(comQty, report, by=c("fao", "m49", "code"), all.x = TRUE)
    comQty <- comQty[is.na(description)!= TRUE,]

    eurQty <- eurQty[,colnames(comQty),with=FALSE]
    res <- rbind(eurQty,comQty[!eurQty,on=c("description", "m49")])

    res1 <- res[, .(mqty = min(qty), mvalue = min(value)), by = list(m49, flow)]#[mqty == 1 | mvalue == 1]

    res2 <- merge(res1, report, by='m49', all.x = TRUE)
    setnames(res2, 'mqty', 'qty')
    setnames(res2, 'mvalue', 'value')

    res3 <- res2[, year:=year]
    reportfinal <- res3[, fao:=NULL]

  } else {
    ## REPORT 6 ###
    eurMissing = data_ce[, .(noqty = sum((is.na(qty_ton) | qty_ton == 0) & (is.na(sup_quantity) | sup_quantity == 0)), tot = .N), .(declarant, flow)]
    eurMissing[, noqty_prop := noqty / tot]
    setnames(eurMissing, 'declarant', 'code')
    eurMissing <- merge(eurMissing, report, by=c("code"), all.x = TRUE)
    eurMissing <- eurMissing[is.na(description)!= TRUE,]

    comMissing = data_ct[, .(noqty = sum((is.na(weight) | weight == 0) & (is.na(qty) | qty == 0)), tot = .N), .(rep, flow)]
    comMissing[, noqty_prop := noqty / tot]
    setnames(comMissing, 'rep', 'code')
    comMissing$code <- as.character(comMissing[,code])
    comMissing <- merge(comMissing, report, by=c("code"), all.x = TRUE)
    comMissing <- comMissing[is.na(description)!= TRUE,]

    eurMissing <- eurMissing[,colnames(comMissing),with=FALSE]
    allMissing <- rbind(eurMissing,comMissing[!eurMissing,on=c("description", "m49")])

    allMissing[flow==1, flow:='import']
    allMissing[flow==2, flow:='export']
    allMissing[flow==3, flow:='reexport']
    allMissing[flow==4, flow:='reimport']

    res <- dcast(allMissing, code+m49+description~flow, value.var = c("noqty", "noqty_prop"))
    reportfinal <- res[, year:=year]

  }

  return(reportfinal)

}

## Prepare the last version of REPORT 1, and save it in R-share folder ###

data_first_report = lapply(year_of_report, createReports, report_number=1)

data_first_report_all = rbindlist(data_first_report)

data_first_report_wide = dcast.data.table(data_first_report_all, m49 + code + description ~ year,value.var = c('is_reporter'))

report_new_1 = data_first_report_wide

report_old_1 <- report_old_1[, colnames(report_old_1)[(colnames(report_old_1) %in% colnames(report_new_1)) &
                                                        (colnames(report_old_1) %!in% c('m49', 'code', 'description'))]:= NULL]

report_1 <- merge(report_old_1, report_new_1, by = c('m49', 'code', 'description'), all.x = TRUE, all.y = TRUE)


for(i in max(names(report_1)[4:ncol(report_1)]):2029){
  report_1[,paste0("year_",i+1)] <- NA
}

colnames(report_1)[4:ncol(report_1)][!grepl('year_',colnames(report_1)[4:ncol(report_1)])] <-
  unlist(paste0("year_", colnames(report_1)[4:ncol(report_1)][!grepl('year_',colnames(report_1)[4:ncol(report_1)])]))


setcolorder(report_1, c("m49", "code", "description", "year_2000", "year_2001", "year_2002", "year_2003", "year_2004", "year_2005",
                        "year_2006", "year_2007", "year_2008", "year_2009", "year_2010", "year_2011", "year_2012", "year_2013",
                        "year_2014", "year_2015", "year_2016", "year_2017", "year_2018", "year_2019", "year_2020", "year_2021",
                        "year_2022", "year_2023", "year_2024", "year_2025", "year_2026", "year_2027", "year_2028", "year_2029", "year_2030"))

saveRDS(report_1, file = file.path(save, "report_1.rds"))

## Prepare the last version of REPORT 2, and save it in R-share folder ###
# 1:imports, 2:exports, 3:re-exports, 4:re-imports

data_second_report = lapply(year_of_report, createReports, report_number=2)

data_second_report_all = rbindlist(data_second_report)

data_second_report_all2 = unique(data_second_report_all[,.(description, code, m49)])

complete_data <-
  CJ(
    description = unique(data_second_report_all$description),
    year = unique(data_second_report_all$year)
  )

data_merged <- merge(complete_data, data_second_report_all2, by=c("description"), all.x =TRUE)

data_merged <- data_merged[!data_second_report_all, on=c("description","year", "m49", "code")]

data_merged[, is_reporter:= 9]

data_merged <- data_merged[,colnames(data_second_report_all),with=FALSE]

new_report <- rbind(data_second_report_all,data_merged[!data_second_report_all, on=c("description","year", "m49", "code")])

data_second_report_wide = dcast.data.table(new_report, m49+code+description~ year,value.var = c('is_reporter'))

report_new_2 = data_second_report_wide

report_old_2 <- report_old_2[, colnames(report_old_2)[(colnames(report_old_2) %in% colnames(report_new_2)) &
                                                        (colnames(report_old_2) %!in% c('m49', 'code', 'description'))]:= NULL]

report_old_2 <- merge(report_old_2, report_new_2, by = c('m49', 'code', 'description'), all.x = TRUE, all.y = TRUE)

report_2 <- report_old_2[!report_new_2, (colnames(report_new_2)[(colnames(report_new_2) %!in% c('m49', 'code', 'description'))]):= 9]

for(i in max(names(report_2)[4:ncol(report_2)]):2029){
  report_2[,paste0("year_",i+1)] <- NA
}

colnames(report_2)[4:ncol(report_2)][!grepl('year_',colnames(report_2)[4:ncol(report_2)])] <-
  unlist(paste0("year_", colnames(report_2)[4:ncol(report_2)][!grepl('year_',colnames(report_2)[4:ncol(report_2)])]))

setcolorder(report_2, c("m49", "code", "description", "year_2000", "year_2001", "year_2002", "year_2003", "year_2004", "year_2005",
                        "year_2006", "year_2007", "year_2008", "year_2009", "year_2010", "year_2011", "year_2012", "year_2013",
                        "year_2014", "year_2015", "year_2016", "year_2017", "year_2018", "year_2019", "year_2020", "year_2021",
                        "year_2022", "year_2023", "year_2024", "year_2025", "year_2026", "year_2027", "year_2028", "year_2029", "year_2030"))

saveRDS(report_2, file = file.path(save, "report_2.rds"))

## Prepare the last version of REPORT 3, and save it in R-share folder ###

data_third_report = lapply(year_of_report, createReports, report_number=3)


data_third_report_all = rbindlist(data_third_report)

data_third_report_all2 = data_third_report_all[order(data_third_report_all$description, data_third_report_all$flow),]
data_third_report_all2 <- na.omit(data_third_report_all2, cols="description")
setnames(data_third_report_all2, 'N', 'records_count')
data_third_report_all3 = data_third_report_all2[ , .(hs_min_diff= ifelse(hs_min==shift(hs_min), FALSE, TRUE),
                                                     hs_max_diff= ifelse(hs_max==shift(hs_max),FALSE, TRUE),
                                                     records_diff=(records_count/shift(records_count)-1),
                                                     code, m49, hs_min, hs_max, hs_n, records_count, year), by=.(description,flow)]

report_new_3 <- data_third_report_all3

report_old_3 = report_old_3[report_old_3$year %!in% report_new_3$year] # Remove the years previously reported. So that they can change with new data

report_3 <- rbind(report_old_3, report_new_3)

report_3 = report_3[order(report_3$description, report_3$flow),] # Repeating the same thing since the report requires comparaison between the years
report_3 <- na.omit(report_3, cols="description")
report_3$records_count <- as.numeric(report_3$records_count)
report_3 = report_3[ , .(hs_min_diff= ifelse(hs_min==shift(hs_min), FALSE, TRUE),
                         hs_max_diff= ifelse(hs_max==shift(hs_max),FALSE, TRUE),
                         records_diff=(records_count/shift(records_count)-1),
                         code, m49, hs_min, hs_max, hs_n, records_count, year), by=.(description,flow)]

setcolorder(report_3, c("m49", "code", "description", "year", "flow", "hs_n", "hs_min", "hs_max", "hs_min_diff",
                        "hs_max_diff", "records_count", "records_diff"))

saveRDS(report_3, file = file.path(save, "report_3.rds"))

## Prepare the last version of REPORT 4, and save it in R-share folder ###

data_forth_report = lapply(year_of_report, createReports, report_number=4)

data_forth_report_all = rbindlist(data_forth_report)

report_new_4 = data_forth_report_all

report_old_4 = report_old_4[report_old_4$year %!in% report_new_4$year] # Remove the years previously reported. So that they can change with new data

report_4 = rbind(report_old_4, report_new_4)

setcolorder(report_4, c("m49", "code", "description", "year", "exports", "imports", "re_exports", "re_imports"))

saveRDS(report_4, file = file.path(save, "report_4.rds"))

## Prepare the last version of REPORT 5, and save it in R-share folder ###

data_fifth_report = lapply(year_of_report, createReports, report_number=5)

data_fifth_report_all = rbindlist(data_fifth_report)

report_new_5 = data_fifth_report_all

report_old_5 = report_old_5[report_old_5$year %!in% report_new_5$year] # Remove the years previously reported. So that they can change with new data

report_5 = rbind(report_old_5, report_new_5)

setcolorder(report_5, c("m49", "code", "description", "year", "flow", "qty", "value"))

saveRDS(report_5, file = file.path(save, "report_5.rds"))

## Prepare the last version of REPORT 6, and save it in R-share folder ###

data_sixth_report = lapply(year_of_report, createReports, report_number=6)

data_sixth_report_all = rbindlist(data_sixth_report, fill = TRUE)

report_new_6 = data_sixth_report_all

report_old_6 = report_old_6[report_old_6$year %!in% report_new_6$year] # Remove the years previously reported. So that they can change with new data

report_6 = rbind(report_old_6, report_new_6, fill=TRUE)

setcolorder(report_6, c("m49", "code", "description", "year", "noqty_export", "noqty_import",
                        "noqty_reexport", "noqty_reimport", "noqty_prop_export", "noqty_prop_import", "noqty_prop_reexport", "noqty_prop_reimport"))

saveRDS(report_6, file = file.path(save, "report_6.rds"))

message("Creating excel file...")

wb <- createWorkbook("Creator of workbook")
addWorksheet(wb, sheetName = "reporter by years")
addWorksheet(wb, sheetName = "non-reporting countries")
addWorksheet(wb, sheetName = "number records by reporter")
addWorksheet(wb, sheetName = "import and export content check")
addWorksheet(wb, sheetName = "check qty and value included")
addWorksheet(wb, sheetName = "missing data by report")


writeData(wb, "reporter by years", report_1, rowNames = FALSE)
writeData(wb, "non-reporting countries", report_2, rowNames = FALSE)
writeData(wb, "number records by reporter", report_3, rowNames = FALSE)
writeData(wb, "import and export content check", report_4, rowNames = FALSE)
writeData(wb, "check qty and value included", report_5, rowNames = FALSE)
writeData(wb, "missing data by report", report_6, rowNames = FALSE)

### SAVE THE EXCEL FORMAT OF REPORTS TO THE SHARED FOLDER AND SEND BY EMAIL TO THE USER ######
saveWorkbook(wb, file = file.path(paste0(save, "/pre-processing reports.", date_of_run, ".xlsx")), overwrite = TRUE)
saveWorkbook(wb, tmp_file_PreProcessing, overwrite = TRUE)

bodyPreProcessing = paste("Plugin completed. The attached excel file contains all the Trade Pre-Processing Reports")

send_mail(from = "no-reply@fao.org", subject = "Trade Pre-Processing Reports", body = c(bodyPreProcessing, tmp_file_PreProcessing), remove = TRUE)

####### SAVE THE DATATABLE ON SWS #######
# Before saving datatable on sws, we should delete the existing ones.

message("Starting to save datatable on SWS...")

allPPRtables <- c("reporters_by_year_new_version", "non_reporting_countries_new_version",
                  "number_records_by_reporter_year_new_version", "import_and_export_content_check_new_version",
                  "check_qty_and_value_included_new_version", "missing_data_by_report_new_version")

files <- list(report_1, report_2, report_3, report_4, report_5, report_6)

for (i in 1:6) {

  tab <- files[[i]]

  ## Delete
  table <- allPPRtables[i]
  changeset <- Changeset(table)
  newdat <- ReadDatatable(table, readOnly = FALSE)

  AddDeletions(changeset, newdat)
  Finalise(changeset)

  ## Add
  AddInsertions(changeset, tab)
  Finalise(changeset)

}

message("Module completed successfully.")

