suppressPackageStartupMessages(library(data.table))
library(faoswsTrade)
library(faosws)
library(faoswsModules)
library(dplyr)



# ## set up for the test environment and parameters
# R_SWS_SHARE_PATH = Sys.getenv("R_SWS_SHARE_PATH")

if(CheckDebug()){
  message("Not on server, so setting up environment...")

  library(faoswsModules)
  SETT <- ReadSettings("C:/Users/Selek/Documents/SWS-Methodology/faoswsTrade/modules/pre_processing_report/sws.yml")

  R_SWS_SHARE_PATH <- SETT[["share"]]
  ## Get SWS Parameters
  SetClientFiles(dir = SETT[["certdir"]])
  GetTestEnvironment(
    baseUrl = SETT[["server"]],
    token = SETT[["token"]]
  )
}

# Read the datasets
# Eurostat data
ce_2014 <- readRDS("C:/Users/Selek/Desktop/TRADE/Data tables/ce_combinednomenclature_unlogged_2014.rds")
# ce_2015 <- readRDS("C:/Users/Selek/Desktop/TRADE/Data tables/ce_combinednomenclature_unlogged_2015.rds")
# ce_2016 <- readRDS("C:/Users/Selek/Desktop/TRADE/Data tables/ce_combinednomenclature_unlogged_2016.rds")
# ce_2017 <- readRDS("C:/Users/Selek/Desktop/TRADE/Data tables/ce_combinednomenclature_unlogged_2017.rds")

# UNSD Data
ct_2014 <- readRDS("C:/Users/Selek/Desktop/TRADE/Data tables/ct_tariffline_unlogged_2014.rds")
# ct_2015 <- readRDS("C:/Users/Selek/Desktop/TRADE/Data tables/ct_tariffline_unlogged_2015.rds")
# ct_2016 <- readRDS("C:/Users/Selek/Desktop/TRADE/Data tables/ct_tariffline_unlogged_2016.rds")
# ct_2017 <- readRDS("C:/Users/Selek/Desktop/TRADE/Data tables/ct_tariffline_unlogged_2017.rds")

`%!in%` = Negate(`%in%`)
options(warn=-1)

country_names <- GetCodeList("trade", "total_trade_cpc_m49", "geographicAreaM49")
stopifnot(nrow(country_names) > 0)

country_names <- country_names[, .(m49 = code, description)]

################# CONVERTING FUNCTIONS ######################

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

eu_codes <- convert_geonom_to_faoandm49(ce_2014)
world_codes <- convert_m49_to_faoandm49(ct_2014)
com_codes <- world_codes[fao %!in% (eu_codes$fao)]

# The column 'code' should contain the Geonom code for eurospean countries and the UNSD M49 codefor the rest
com_codes <- com_codes[,colnames(eu_codes),with=FALSE]
forreport <- rbind(eu_codes,com_codes[!eu_codes,on=c("fao", "m49")])

report <- merge(forreport, country_names, by='m49' )


####### REPORT 1: Reporters by year ##############
# This function creates first report yearly basis
reportersByYear <- function(report=report, EurostatData=ce_2014, ComTradeData=ct_2014){

  report1 <- copy(report)
  yearvalue <- substr(EurostatData$period[1], start=1,4)
  year <- paste0('year_',yearvalue)

  reportersList1 <- unique(eu_codes[code %in% unique(EurostatData[, declarant]), m49])
  reportersList2 <- unique(com_codes[code %in% unique(ComTradeData[, rep]), m49])
  reportersList <- unique(c(reportersList1, reportersList2))

  report1 <- report1[, get("year"):=ifelse(m49 %in% reportersList,'X',NA)]

  report1 <- report1[, fao:=NULL]

  return(report1)
}

report_1 <- reportersByYear(report, ce_2014, ct_2014)


####### REPORT 2: Non-reporting countries ##############
# This function creates second report yearly basis
# 1:imports, 2:exports, 3:re-exports, 4:re-imports

nonReportingCountries <- function(report=report, EurostatData=ce_2014, ComTradeData=ct_2014){

  report2 <- copy(report)
  yearvalue <- substr(EurostatData$period[1], start=1,4)
  year <- paste0('year_',yearvalue)

  EU_imports <- unique(eu_codes[code %in% unique(EurostatData[flow=='1', declarant]), m49])
  EU_exports <- unique(eu_codes[code %in% unique(EurostatData[flow=='2', declarant]), m49])

  Com_imports <- unique(com_codes[code %in% unique(ComTradeData[flow=='1'| flow=='4', rep]), m49])
  Com_exports <- unique(com_codes[code %in% unique(ComTradeData[flow=='2'| flow=='3', rep]), m49])

  imports_total <- unique(c(EU_imports, Com_imports))
  exports_total <- unique(c(EU_exports, Com_exports))

  report2 <- report2[, get("year"):=ifelse(m49 %!in% imports_total & m49 %!in% exports_total,9,
                                         ifelse(m49 %!in% imports_total & m49 %in% exports_total, 1,
                                                ifelse(m49 %in% imports_total & m49 %!in% exports_total,2,NA)))]
  report2 <- report2[, fao:=NULL]

  return(report2)
}

report_2 <- nonReportingCountries(report, ce_2014, ct_2014)


####### REPORT 3: Number records by reporter/year ##############
# This function creates third report yearly basis

numberOfRecordsReporter <- function(report=report, EurostatData=ce_2014, ComTradeData=ct_2014){

  report3 <- copy(report)
  yearvalue <- ComTradeData$tyear[1]

  # Adding hs_min: the minimum length of HS digits,
  #        hs_max: the maximum length of HS digits,
  #        hs_n: how many different digits the country used.

  eurNumRec <- EurostatData[,.(hs_min= min(nchar(product_nc)),
                                hs_max=max(nchar(product_nc)),
                                hs_n= length(unique(nchar(product_nc))), .N), by=.(declarant, flow)]
  setnames(eurNumRec, 'declarant', 'code')
  eurNumRec <- merge(eurNumRec, eu_codes, by='code', all.x = TRUE)
  #eurNumRec <- eurNumRec[, rep_name:=NULL]
  eurNumRec <- merge(eurNumRec, report3, by=c("fao","code", 'm49'), all.x = TRUE)

  comNumRec <- ComTradeData[,.(hs_min= min(nchar(comm)),
                                hs_max=max(nchar(comm)),
                                hs_n= length(unique(nchar(comm))), .N), by=.(rep, flow)]
  setnames(comNumRec, 'rep', 'code')
  comNumRec$code <- as.numeric(as.character(comNumRec[,code]))
  comNumRec <- merge(comNumRec, com_codes, by='code', all.y = TRUE)
  #comNumRec <- comNumRec[, rep_name:=NULL]
  comNumRec$code <- as.character(comNumRec[,code])
  comNumRec <- merge(comNumRec, report3, by=c("fao","code", 'm49'), all.x = TRUE)

  eurNumRec <- eurNumRec[,colnames(comNumRec),with=FALSE]
  report3 <- rbind(eurNumRec,comNumRec[!eurNumRec,on=c("description", "m49")])

  report3 <- report3[,year:= rep(get('yearvalue'),nrow(report3))]

  return(report3)
}

report_3 <- numberOfRecordsReporter(report, ce_2014, ct_2014)


####### REPORT 4: Import and export content check ##############
# This function creates forth report yearly basis

importExportContentCheck <- function(report=report, EurostatData=ce_2014, ComTradeData=ct_2014){

  report4 <- copy(report)
  yearvalue <- ComTradeData$tyear[1]

  eurTrans <- EurostatData[,.N, by=.(declarant, flow)]
  setnames(eurTrans, 'declarant', 'code')
  eurTrans <- merge(eurTrans, eu_codes, by='code', all.x = TRUE)
  #eurTrans <- eurTrans[, rep_name:=NULL]
  eurTrans <- eurTrans[, N:=NULL]
  eurTrans <- merge(eurTrans, report4, by=c("fao","m49","code"), all.x = TRUE)

  comTrans <- ComTradeData[,.N, by=.(rep, flow)]
  setnames(comTrans, 'rep', 'code')
  comTrans$code <- as.numeric(as.character(comTrans[,code]))
  comTrans <- merge(comTrans, com_codes, by='code', all.y = TRUE)
  #comTrans <- comTrans[, rep_name:=NULL]
  comTrans <- comTrans[, N:=NULL]
  comTrans$code <- as.character(comTrans[,code])
  comTrans <- merge(comTrans, report4, by=c("fao", "m49", "code"), all.x = TRUE)

  eurTrans <- eurTrans[,colnames(comTrans),with=FALSE]
  ans <- rbind(eurTrans,comTrans[!eurTrans,on=c("description", "m49")])

  ans1 <- ans[flow==1, .(code, m49, fao)]
  ans2 <- ans[flow==2, .(code, m49, fao)]
  ans3 <- ans[flow==3, .(code, m49, fao)]
  ans4 <- ans[flow==4, .(code, m49, fao)]
  # 1:imports, 2:exports, 3:re-exports, 4:re-imports
  report4 <- report4[, exports:= ifelse(m49 %in% ans2$m49,'X',NA)]
  report4 <- report4[, imports:= ifelse(m49 %in% ans1$m49,'X',NA)]
  report4 <- report4[, re_exports:= ifelse(m49 %in% ans3$m49,'X',NA)]
  report4 <- report4[, re_imports:= ifelse(m49 %in% ans4$m49,'X',NA)]

  report4 <- report4[,year:= rep(get('yearvalue'),nrow(report4))]

  return(report4)
}

report_4 <- importExportContentCheck(report, ce_2014, ct_2014)

####### REPORT 5: Check qty and value included ##############
# This function creates fifth report yearly basis

checkQtyValue <- function(report=report, EurostatData=ce_2014, ComTradeData=ct_2014){

  report5 <- copy(report)
  yearvalue <- ComTradeData$tyear[1]

  eurQty <- EurostatData[,.(qty=qty_ton, value=value_1k_euro), by=.(declarant, flow)]
  eurQty <- eurQty[, check_info:= ifelse(qty==0 & value==0, FALSE, TRUE)]
  setnames(eurQty, 'declarant', 'code')
  eurQty <- merge(eurQty, eu_codes, by='code', all.x = TRUE)
  eurQty <- merge(eurQty, report5, by=c("fao", "m49", "code"), all.x = TRUE)

  comQty <- ComTradeData[,.(qty, value=tvalue), by=.(rep, flow)]
  comQty <- comQty[, check_info:= ifelse(qty==0 & value==0, FALSE, TRUE)]
  setnames(comQty, 'rep', 'code')
  comQty$code <- as.numeric(as.character(comQty[,code]))
  comQty <- merge(comQty, com_codes, by='code', all.y = TRUE)
  comQty$code <- as.character(comQty[,code])
  comQty <- merge(comQty, report5, by=c("fao", "m49", "code"), all.x = TRUE)



  eurQty <- eurQty[,colnames(comQty),with=FALSE]
  res <- rbind(eurQty,comQty[!eurQty,on=c("fao", "m49", "description")])
  res1 <- res[,.N , by= .(fao, m49, code, description, flow)]
  res2 <- res[,.N , by= .(fao, m49, code, description, flow, check_info)]

  res3 <- res1[, qty:= ifelse(res2$check_info==FALSE ,1,0)]
  res4 <- res3[, value:= ifelse(res2$check_info==FALSE, 1,0)]


  #
  # res <- res[, .N, by=.(fao, m49, code, description, flow, qty, value)]
  # res <- res[, N:=NULL]
  # res5 <- res[, `:=`(qty2= ifelse(qty %in% 1, 1,0), value2= ifelse(value %in% 1, 1,0)), by= .(flow)]

  res5 <- res4[,year:= rep(get('yearvalue'),nrow(res4))]

  return(res5)
}

report_5 <- checkQtyValue(report, ce_2014, ct_2014)

####### REPORT 6: Missing data by report ##############
# This function creates sixth report yearly basis

missingDataByReport <- function(report=report, EurostatData=ce_2014, ComTradeData=ct_2014) {

  report6 <- copy(report)
  yearvalue <- ComTradeData$tyear[1]

  eurMissing <- EurostatData[,.N, by=.(declarant, qty_ton, sup_quantity, flow)]
  setnames(eurMissing, 'declarant', 'code')
  eurMissing <- merge(eurMissing, eu_codes, by='code', all.x = TRUE)
  eurMissing <- eurMissing[, N:=NULL]
  eurMissing <- merge(eurMissing, report6, by=c("fao","m49","code"), all.x = TRUE)

  comMissing <- ComTradeData[,.N, by=.(rep, weight, qty, flow)]
  setnames(comMissing, 'rep', 'code')
  comMissing$code <- as.numeric(as.character(comMissing[,code]))
  comMissing <- merge(comMissing, com_codes, by='code', all.y = TRUE)
  comMissing <- comMissing[, N:=NULL]
  comMissing$code <- as.character(comMissing[,code])
  comMissing <- merge(comMissing, report6, by=c("fao", "m49", "code"), all.x = TRUE)

  setnames(eurMissing, 'qty_ton', 'weight')
  setnames(eurMissing, 'sup_quantity', 'qty')

  eurMissing <- eurMissing[,colnames(comMissing),with=FALSE]
  ans <- rbind(eurMissing,comMissing[!eurMissing,on=c("description", "m49", "fao")])

  ans1 <- ans[flow==1 & weight==0 & qty==0, .N, by=.(m49)]
  ans2 <- ans[flow==2 & weight==0 & qty==0, .N, by=.(m49)]
  ans3 <- ans[flow==3 & weight==0 & qty==0, .N, by=.(m49)]
  ans4 <- ans[flow==4 & weight==0 & qty==0, .N, by=.(m49)]

  ans5 <- ans[flow==1, .N, by=.(m49)]
  ans6 <- ans[flow==2, .N, by=.(m49)]
  ans7 <- ans[flow==3, .N, by=.(m49)]
  ans8 <- ans[flow==4, .N, by=.(m49)]

  report6 <- report6[, noqty_export:=ifelse(m49 %in% ans2$m49, ans2$N, NA)]
  report6 <- report6[, noqty_import:=ifelse(m49 %in% ans1$m49, ans1$N, NA)]
  report6 <- report6[, noqty_reexport:=ifelse(m49 %in% ans3$m49, ans3$N, NA)]
  report6 <- report6[, noqty_reimport:=ifelse(m49 %in% ans4$m49, ans4$N, NA)]

  report6 <- report6[m49 %in% ans2$m49, noqty_prop_export:= ans2$N/ans6$N]
  report6 <- report6[m49 %in% ans1$m49, noqty_prop_import:= ans1$N/ans5$N]
  report6 <- report6[m49 %in% ans3$m49, noqty_prop_reexport:=ans3$N/ans7$N]
  report6 <- report6[m49 %in% ans4$m49, noqty_prop_reimport:=ans4$N/ans8$N]

  report6 <- report6[,year:= rep(get('yearvalue'),nrow(report6))]

  return(report6)

}

report_6 <- missingDataByReport(report, ce_2014, ct_2014)

