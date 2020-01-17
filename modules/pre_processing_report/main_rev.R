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



####### REPORT 1: Reporters by year ##############
firstReport<- function(year, names=country_names){

  file_ce= paste0('ce_combinednomenclature_unlogged_', year, '.rds')
  file_ct= paste0('ct_tariffline_unlogged_', year, '.rds')

  file_path_ce= paste0('C:/Users/Selek/Desktop/TRADE/raw-data/', file_ce)
  file_path_ct= paste0('C:/Users/Selek/Desktop/TRADE/raw-data/', file_ct)

  data_ce= readRDS(file_path_ce)
  data_ct= readRDS(file_path_ct)
  setDT(data_ce)
  setDT(data_ct)


  hs_chapters <- c("01","02", "03", "04", "05", "06","07", "08", "09", "10", "11", "12","13", "14", "15", "16",
                   "17", "18", "19", "20", "21", "22", "23", "24", "33", "35", "38", "40", "41", "43", "50", "51", "52", "53")

  data_ce <- data_ce[, chapter:= substr(product_nc, start=1,2)]
  data_ce <- data_ce[chapter %in% hs_chapters]
  data_ct <- data_ct[chapter %in% hs_chapters]

  data_ce <- data_ce[, remover:=ifelse(nchar(product_nc)==2, NA, product_nc)]
  data_ce <- na.omit(data_ce, cols="remover")
  data_ct <- data_ct[, remover:=ifelse(nchar(comm)==2, NA, comm)]
  data_ct <- na.omit(data_ct, cols="remover")

  eu_codes <- convert_geonom_to_faoandm49(data_ce)
  world_codes <- convert_m49_to_faoandm49(data_ct)
  com_codes <- world_codes[fao %!in% (eu_codes$fao)]

  # The column 'code' should contain the Geonom code for eurospean countries and the UNSD M49 codefor the rest
  com_codes <- com_codes[,colnames(eu_codes),with=FALSE]
  forreport <- rbind(eu_codes,com_codes[!eu_codes,on=c("fao", "m49")])

  report <- merge(forreport, names, by='m49' )

  reportersList1 <- unique(eu_codes[code %in% unique(data_ce[, declarant]), m49])
  reportersList2 <- unique(com_codes[code %in% unique(data_ct[, rep]), m49])
  reportersList <- unique(c(reportersList1, reportersList2))

  report <- report[, is_reporter:=ifelse(m49 %in% reportersList,'X',NA)]

  report <- report[, fao:=NULL]
  report <- report[, year:=year]
  #report <- report[, element:='report1']

  return(report)

}

yeartest = (2000:2018)

data_first_report = lapply(yeartest, firstReport)

data_first_report_all = rbindlist(data_first_report)

data_first_report_wide = dcast.data.table(data_first_report_all, m49+code+description~ year,value.var = c('is_reporter'))

report_1 = data_first_report_wide

saveRDS(report_1, file = "report_1.rds")

####### REPORT 2: Non-reporting countries ##############
# 1:imports, 2:exports, 3:re-exports, 4:re-imports

secondReport<- function(year, names=country_names){
  #year=2000
  file_ce= paste0('ce_combinednomenclature_unlogged_', year, '.rds')
  file_ct= paste0('ct_tariffline_unlogged_', year, '.rds')

  file_path_ce= paste0('C:/Users/Selek/Desktop/TRADE/raw-data/', file_ce)
  file_path_ct= paste0('C:/Users/Selek/Desktop/TRADE/raw-data/', file_ct)

  data_ce= readRDS(file_path_ce)
  data_ct= readRDS(file_path_ct)
  setDT(data_ce)
  setDT(data_ct)


  hs_chapters <- c("01","02", "03", "04", "05", "06","07", "08", "09", "10", "11", "12","13", "14", "15", "16",
                   "17", "18", "19", "20", "21", "22", "23", "24", "33", "35", "38", "40", "41", "43", "50", "51", "52", "53")

  data_ce <- data_ce[, chapter:= substr(product_nc, start=1,2)]
  data_ce <- data_ce[chapter %in% hs_chapters]
  data_ct <- data_ct[chapter %in% hs_chapters]

  data_ce <- data_ce[, remover:=ifelse(nchar(product_nc)==2, NA, product_nc)]
  data_ce <- na.omit(data_ce, cols="remover")
  data_ct <- data_ct[, remover:=ifelse(nchar(comm)==2, NA, comm)]
  data_ct <- na.omit(data_ct, cols="remover")

  eu_codes <- convert_geonom_to_faoandm49(data_ce)
  world_codes <- convert_m49_to_faoandm49(data_ct)
  com_codes <- world_codes[fao %!in% (eu_codes$fao)]

  # The column 'code' should contain the Geonom code for eurospean countries and the UNSD M49 codefor the rest
  com_codes <- com_codes[,colnames(eu_codes),with=FALSE]
  forreport <- rbind(eu_codes,com_codes[!eu_codes,on=c("fao", "m49")])

  report <- merge(forreport, names, by='m49' )

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
  report <- report[, year:=year]

  return(report)

}

yeartest = (2000:2018)

data_second_report = lapply(yeartest, secondReport)

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

report_2 = data_first_report_wide

saveRDS(report_2, file = "report_2.rds")

####### REPORT 3: Number records by reporter/year ##############
thirdReport<- function(year, names=country_names){

  file_ce= paste0('ce_combinednomenclature_unlogged_', year, '.rds')
  file_ct= paste0('ct_tariffline_unlogged_', year, '.rds')

  file_path_ce= paste0('C:/Users/Selek/Desktop/TRADE/raw-data/', file_ce)
  file_path_ct= paste0('C:/Users/Selek/Desktop/TRADE/raw-data/', file_ct)

  data_ce= readRDS(file_path_ce)
  data_ct= readRDS(file_path_ct)
  setDT(data_ce)
  setDT(data_ct)


  hs_chapters <- c("01","02", "03", "04", "05", "06","07", "08", "09", "10", "11", "12","13", "14", "15", "16",
                   "17", "18", "19", "20", "21", "22", "23", "24", "33", "35", "38", "40", "41", "43", "50", "51", "52", "53")

  data_ce <- data_ce[, chapter:= substr(product_nc, start=1,2)]
  data_ce <- data_ce[chapter %in% hs_chapters]
  data_ct <- data_ct[chapter %in% hs_chapters]

  data_ce <- data_ce[, remover:=ifelse(nchar(product_nc)==2, NA, product_nc)]
  data_ce <- na.omit(data_ce, cols="remover")
  data_ct <- data_ct[, remover:=ifelse(nchar(comm)==2, NA, comm)]
  data_ct <- na.omit(data_ct, cols="remover")

  eu_codes <- convert_geonom_to_faoandm49(data_ce)
  world_codes <- convert_m49_to_faoandm49(data_ct)
  com_codes <- world_codes[fao %!in% (eu_codes$fao)]

  # The column 'code' should contain the Geonom code for eurospean countries and the UNSD M49 codefor the rest
  com_codes <- com_codes[,colnames(eu_codes),with=FALSE]
  forreport <- rbind(eu_codes,com_codes[!eu_codes,on=c("fao", "m49")])

  report <- merge(forreport, names, by='m49' )

  eurNumRec <- data_ce[,.(hs_min= min(nchar(product_nc)),
                          hs_max=max(nchar(product_nc)),
                          hs_n= length(unique(nchar(product_nc))), .N), by=.(declarant, flow)]
  setnames(eurNumRec, 'declarant', 'code')
  eurNumRec <- merge(eurNumRec, eu_codes, by='code', all.x = TRUE)
  eurNumRec <- merge(eurNumRec, report, by=c("fao","code", 'm49'), all.x = TRUE)
  eurNumRec <- eurNumRec[is.na(description)!= TRUE,]

  comNumRec <- data_ct[,.(hs_min= min(nchar(comm)),
                          hs_max=max(nchar(comm)),
                          hs_n= length(unique(nchar(comm))), .N), by=.(rep, flow)]
  setnames(comNumRec, 'rep', 'code')
  comNumRec$code <- as.numeric(as.character(comNumRec[,code]))
  comNumRec <- merge(comNumRec, com_codes, by='code', all.y = TRUE)

  comNumRec$code <- as.character(comNumRec[,code])
  comNumRec <- merge(comNumRec, report, by=c("fao","code", 'm49'), all.x = TRUE)

  eurNumRec <- eurNumRec[,colnames(comNumRec),with=FALSE]
  report <- rbind(eurNumRec,comNumRec[!eurNumRec,on=c("description", "m49")])

  report <- report[, fao:=NULL]
  report <- report[, year:=year]

  return(report)

}

yeartest = (2000:2018)

data_third_report = lapply(yeartest, thirdReport)

data_third_report_all = rbindlist(data_third_report)

data_third_report_all2 = data_third_report_all[order(data_third_report_all$description, data_third_report_all$flow),]
data_third_report_all2 <- na.omit(data_third_report_all2, cols="description")
setnames(data_third_report_all2, 'N', 'records_count')
data_third_report_all3 = data_third_report_all2[ , .(hs_min_diff= ifelse(hs_min==shift(hs_min), FALSE, TRUE),
                                                     hs_max_diff= ifelse(hs_max==shift(hs_max),FALSE, TRUE),
                                                     records_diff=(records_count/shift(records_count)-1),
                                                     code, m49, hs_min, hs_max, hs_n, records_count, year), by=.(description,flow)]

report_3 = data_third_report_all3

saveRDS(report_3, file = "report_3.rds")

####### REPORT 4: Import and export content check ##############
forthReport<- function(year, names=country_names){
  file_ce= paste0('ce_combinednomenclature_unlogged_', year, '.rds')
  file_ct= paste0('ct_tariffline_unlogged_', year, '.rds')

  file_path_ce= paste0('C:/Users/Selek/Desktop/TRADE/raw-data/', file_ce)
  file_path_ct= paste0('C:/Users/Selek/Desktop/TRADE/raw-data/', file_ct)

  data_ce= readRDS(file_path_ce)
  data_ct= readRDS(file_path_ct)
  setDT(data_ce)
  setDT(data_ct)


  hs_chapters <- c("01","02", "03", "04", "05", "06","07", "08", "09", "10", "11", "12","13", "14", "15", "16",
                   "17", "18", "19", "20", "21", "22", "23", "24", "33", "35", "38", "40", "41", "43", "50", "51", "52", "53")

  data_ce <- data_ce[, chapter:= substr(product_nc, start=1,2)]
  data_ce <- data_ce[chapter %in% hs_chapters]
  data_ct <- data_ct[chapter %in% hs_chapters]

  data_ce <- data_ce[, remover:=ifelse(nchar(product_nc)==2, NA, product_nc)]
  data_ce <- na.omit(data_ce, cols="remover")
  data_ct <- data_ct[, remover:=ifelse(nchar(comm)==2, NA, comm)]
  data_ct <- na.omit(data_ct, cols="remover")

  eu_codes <- convert_geonom_to_faoandm49(data_ce)
  world_codes <- convert_m49_to_faoandm49(data_ct)
  com_codes <- world_codes[fao %!in% (eu_codes$fao)]

  # The column 'code' should contain the Geonom code for eurospean countries and the UNSD M49 codefor the rest
  com_codes <- com_codes[,colnames(eu_codes),with=FALSE]
  forreport <- rbind(eu_codes,com_codes[!eu_codes,on=c("fao", "m49")])

  report <- merge(forreport, names, by='m49' )

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
  report <- report[, fao:=NULL]

  return(report)

}

yeartest = (2000:2018)

data_forth_report = lapply(yeartest, forthReport)

data_forth_report_all = rbindlist(data_forth_report)

report_4 = data_forth_report_all

saveRDS(report_4, file = "report_4.rds")

####### REPORT 5: Check qty and value included ##############
fifthReport<- function(year, names=country_names){
  file_ce= paste0('ce_combinednomenclature_unlogged_', year, '.rds')
  file_ct= paste0('ct_tariffline_unlogged_', year, '.rds')

  file_path_ce= paste0('C:/Users/Selek/Desktop/TRADE/raw-data/', file_ce)
  file_path_ct= paste0('C:/Users/Selek/Desktop/TRADE/raw-data/', file_ct)

  data_ce= readRDS(file_path_ce)
  data_ct= readRDS(file_path_ct)
  setDT(data_ce)
  setDT(data_ct)


  hs_chapters <- c("01","02", "03", "04", "05", "06","07", "08", "09", "10", "11", "12","13", "14", "15", "16",
                   "17", "18", "19", "20", "21", "22", "23", "24", "33", "35", "38", "40", "41", "43", "50", "51", "52", "53")

  data_ce <- data_ce[, chapter:= substr(product_nc, start=1,2)]
  data_ce <- data_ce[chapter %in% hs_chapters]
  data_ct <- data_ct[chapter %in% hs_chapters]

  data_ce <- data_ce[, remover:=ifelse(nchar(product_nc)==2, NA, product_nc)]
  data_ce <- na.omit(data_ce, cols="remover")
  data_ct <- data_ct[, remover:=ifelse(nchar(comm)==2, NA, comm)]
  data_ct <- na.omit(data_ct, cols="remover")

  eu_codes <- convert_geonom_to_faoandm49(data_ce)
  world_codes <- convert_m49_to_faoandm49(data_ct)
  com_codes <- world_codes[fao %!in% (eu_codes$fao)]

  # The column 'code' should contain the Geonom code for eurospean countries and the UNSD M49 codefor the rest
  com_codes <- com_codes[,colnames(eu_codes),with=FALSE]
  forreport <- rbind(eu_codes,com_codes[!eu_codes,on=c("fao", "m49")])

  report <- merge(forreport, names, by='m49' )

  eurQty = data_ce[, .(qty = ifelse((is.na(qty_ton) | qty_ton == 0) & (is.na(sup_quantity) | sup_quantity == 0),1,0),
                       value = ifelse((is.na(value_1k_euro) | value_1k_euro == 0),1,0)), .(declarant, flow)]

  setnames(eurQty, 'declarant', 'code')
  eurQty <- merge(eurQty, eu_codes, by='code', all.x = TRUE)
  eurQty <- merge(eurQty, report, by=c("fao", "m49", "code"), all.x = TRUE)
  eurQty <- eurQty[is.na(description)!= TRUE,]

  comQty = data_ct[, .(qty = ifelse((is.na(weight) | weight == 0) & (is.na(qty) | qty == 0),1,0),
                       value = ifelse((is.na(tvalue) | tvalue == 0),1,0)), .(rep, flow)]
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
  res3 <- res3[, fao:=NULL]

  return(res3)

}

yeartest = (2000:2018)

data_fifth_report = lapply(yeartest, fifthReport)

data_fifth_report_all = rbindlist(data_fifth_report)

report_5 = data_fifth_report_all

saveRDS(report_5, file = "report_5.rds")

####### REPORT 6: Missing data by report ##############
sixthReport<- function(year, names=country_names){
  file_ce= paste0('ce_combinednomenclature_unlogged_', year, '.rds')
  file_ct= paste0('ct_tariffline_unlogged_', year, '.rds')

  file_path_ce= paste0('C:/Users/Selek/Desktop/TRADE/raw-data/', file_ce)
  file_path_ct= paste0('C:/Users/Selek/Desktop/TRADE/raw-data/', file_ct)

  data_ce= readRDS(file_path_ce)
  data_ct= readRDS(file_path_ct)
  setDT(data_ce)
  setDT(data_ct)

  hs_chapters <- c("01","02", "03", "04", "05", "06","07", "08", "09", "10", "11", "12","13", "14", "15", "16",
                   "17", "18", "19", "20", "21", "22", "23", "24", "33", "35", "38", "40", "41", "43", "50", "51", "52", "53")

  data_ce <- data_ce[, chapter:= substr(product_nc, start=1,2)]
  data_ce <- data_ce[chapter %in% hs_chapters]
  data_ct <- data_ct[chapter %in% hs_chapters]

  data_ce <- data_ce[, remover:=ifelse(nchar(product_nc)==2, NA, product_nc)]
  data_ce <- na.omit(data_ce, cols="remover")
  data_ct <- data_ct[, remover:=ifelse(nchar(comm)==2, NA, comm)]
  data_ct <- na.omit(data_ct, cols="remover")

  eu_codes <- convert_geonom_to_faoandm49(data_ce)
  world_codes <- convert_m49_to_faoandm49(data_ct)
  com_codes <- world_codes[fao %!in% (eu_codes$fao)]

  # The column 'code' should contain the Geonom code for eurospean countries and the UNSD M49 codefor the rest
  com_codes <- com_codes[,colnames(eu_codes),with=FALSE]
  forreport <- rbind(eu_codes,com_codes[!eu_codes,on=c("fao", "m49")])

  report <- merge(forreport, names, by='m49' )

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
  res <- res[, year:=year]

  return(res)

}
yeartest = (2000:2018)

data_sixth_report = lapply(yeartest, sixthReport)

data_sixth_report_all = rbindlist(data_sixth_report)

report_6 = data_sixth_report_all

saveRDS(report_6, file = "report_6.rds")

####### CREATE EXCEL FILE #######

library(openxlsx)

wb <- createWorkbook("Creator of workbook")
addWorksheet(wb, sheetName = "reporter by years")
addWorksheet(wb, sheetName = "non-reporting countries")
addWorksheet(wb, sheetName = "number records by reporter")
addWorksheet(wb, sheetName = "import and export content check")
addWorksheet(wb, sheetName = "check qty and value included")
addWorksheet(wb, sheetName = "missing data by report")


writeData(wb, "reporter by years", report_1, startCol = 1, startRow = 1, rowNames = TRUE)
writeData(wb, "non-reporting countries", report_2, startCol = 1, startRow = 1, rowNames = TRUE)
writeData(wb, "number records by reporter", report_3, startCol = 1, startRow = 1, rowNames = TRUE)
writeData(wb, "import and export content check", report_4, startCol = 1, startRow = 1, rowNames = TRUE)
writeData(wb, "check qty and value included", report_5, startCol = 1, startRow = 1, rowNames = TRUE)
writeData(wb, "missing data by report", report_6, startCol = 1, startRow = 1, rowNames = TRUE)

saveWorkbook(wb, file = "C:/Users/Selek/Dropbox/1-FAO-DROPBOX/faosws_Trade/pre-processing reports.v0.3.xlsx", overwrite = TRUE)

