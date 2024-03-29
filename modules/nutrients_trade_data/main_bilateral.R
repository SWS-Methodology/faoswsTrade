##'
##' **Author: Aydan Selek**
##'
##' **Description:**
##'
##' * Trade nutrients
##'
##'
##' **Inputs:**
##'
##' * bilateral (complete) trade data
##'
##' **Flag assignment:**
##'
##' E,e

message("TRADE: Nutrients calculation for bilateral trade data is starting...")

## Load the libraries
library(faosws)
suppressPackageStartupMessages(library(data.table))
library(faoswsUtil)
library(sendmailR)
suppressPackageStartupMessages(library(dplyr, warn.conflicts = FALSE))

# If this is set to TRUE, the module will download the whole dataset
# saved on SWS and will do a setdiff by comparing this
# set and the dataset generated by the module.

`%!in%` = Negate(`%in%`)
remove_nonexistent_transactions <- TRUE

local({
  min_versions <- data.frame(package = c("faoswsFlag", "faoswsTrade"),
                             version = c('0.2.4', '0.1.1'),
                             stringsAsFactors = FALSE)

  for (i in nrow(min_versions)){
    # installed version
    p <- packageVersion(min_versions[i,"package"])
    # required version
    v <- package_version(min_versions[i,"version"])
    if(p < v){

      stop(sprintf("%s >= %s required", min_versions[i,"package"], v))
    }
  }

})



if (CheckDebug()) {
  library(faoswsModules)
  SETTINGS = ReadSettings("modules/nutrients_trade_data/sws.yml")
  ## Define where your certificates are stored
  faosws::SetClientFiles(SETTINGS[["certdir"]])
  ## Get session information from SWS. Token must be obtained from web interface
  GetTestEnvironment(baseUrl = SETTINGS[["server"]],
                     token = SETTINGS[["token"]])
}

## TODO: think over this if we need to insert it as parameter
min_year = as.numeric(swsContext.computationParams$min_year)
max_year = as.numeric(swsContext.computationParams$max_year)
years <- as.integer(min_year:max_year)



## GET BILATERAL TRADE DATA
bil_reportersDim =
  GetCodeList("trade", "completed_tf_cpc_m49", "geographicAreaM49Reporter")[type == "country", code] %>%
  Dimension(name = "geographicAreaM49Reporter", keys = .)

bil_partnersDim =
  GetCodeList("trade", "completed_tf_cpc_m49", "geographicAreaM49Partner")[type == "country", code] %>%
  Dimension(name = "geographicAreaM49Partner", keys = .)

bil_eleDim =
  c("5610", "5910") %>%
  Dimension(name = "measuredElementTrade", keys = .)

bil_itemDim =
  GetCodeList("trade", "completed_tf_cpc_m49", "measuredItemCPC")[,code] %>%
  Dimension(name = "measuredItemCPC", keys = .)

bil_timeDim <- Dimension(name = "timePointYears", keys = as.character(years))

bilateraltrade_key <- DatasetKey(domain = "trade", dataset = "completed_tf_cpc_m49", dimensions = list(
  geographicAreaM49Reporter = bil_reportersDim,
  geographicAreaM49Partner = bil_partnersDim,
  measuredElementTrade = bil_eleDim,
  measuredItemCPC = bil_itemDim,
  timePointYears = bil_timeDim
))

completetrade <- GetData(bilateraltrade_key)


## GET GLOBAL NCT DATASET
glo_geoDim =
  GetCodeList(domain = "suafbs", dataset = "global_nct", dimension = "geographicAreaM49")[description == "wildcard", code] %>%
  Dimension(name = "geographicAreaM49", keys = .)

glo_eleDim =
  GetCodeList(domain = "suafbs", dataset = "global_nct", "measuredElement")[,code] %>%
  Dimension(name = "measuredElement", keys = .)

glo_itemDim =
  GetCodeList(domain = "suafbs", dataset = "global_nct", "measuredItemCPC")[,code] %>%
  Dimension(name = "measuredItemCPC", keys = .)

glo_timeDim =
  Dimension(name = "timePointYearsSP", keys = as.character(0))

global_nct_key = DatasetKey(domain = "suafbs", dataset = "global_nct", dimensions = list(
  geographicAreaM49 = glo_geoDim,
  measuredElementTrade = glo_eleDim,
  measuredItemCPC = glo_itemDim,
  timePointYears = glo_timeDim
))

global_nct <- GetData(global_nct_key)


## GET POPULATION DATA
population_key <- DatasetKey(domain = "population", dataset = "population_unpd", dimensions = list(
        geographicAreaM49 =
          GetCodeList(domain = "trade", dataset = "total_trade_cpc_m49", dimension = "geographicAreaM49")[type == "country", code] %>%
          Dimension(name = "geographicAreaM49", keys = .),
        measuredElementSuaFbs = Dimension(name = "measuredElement", keys = "511"), # 511 = Total population
        timePointYears = Dimension(name = "timePointYears", keys = as.character(years))
      ))

population <- GetData(population_key)
population <- population[geographicAreaM49 != "156",]

# Fix for missing regional official data in the country total
# Source: DEMOGRAPHIC SURVEY, Kurdistan Region of Iraq, July 2018, IOM UN Migration
# ("the KRI population at 5,122,747 individuals and the overall Iraqi
# population at 36,004,552 individuals", pag.14; it implies 14.22805%)
# https://iraq.unfpa.org/sites/default/files/pub-pdf/KRSO%20IOM%20UNFPA%20Demographic%20Survey%20Kurdistan%20Region%20of%20Iraq_0.pdf
#population[geographicAreaM49 == "368" , Value := Value * 0.8577195]

# Flags will be totally discarded.
population[, c('flagObservationStatus', 'flagMethod'):= NULL]
completetrade[, c('flagObservationStatus', 'flagMethod'):= NULL]
global_nct [, c('flagObservationStatus', 'flagMethod'):= NULL]


# TODO: For now, I will deal with only the coutries (areas) we have population information
completetrade <- completetrade[geographicAreaM49Reporter %in% unique(population$geographicAreaM49), ]
population <- population[geographicAreaM49 %in% unique(completetrade$geographicAreaM49Reporter), ]

completetrade <- completetrade[Value!=0,]

completetrade_dcast <- dcast.data.table(completetrade, geographicAreaM49Reporter +
                                          geographicAreaM49Partner + measuredItemCPC + timePointYears
                                     ~ measuredElementTrade, value.var = c('Value'))
population[,measuredElement:=NULL]
setnames(population, 'Value', 'Population')
completetrade_pop <- merge(completetrade_dcast, population, by.x = c('geographicAreaM49Reporter', 'timePointYears'),
                           by.y = c('geographicAreaM49', 'timePointYears'), all.x = TRUE)

# length(unique(totaltrade$measuredItemCPC))
# length(unique(global_fct$CPC_Code))
# totalites <- unique(totaltrade$measuredItemCPC)
# globalitems <- unique(global_fct$CPC_Code)
# totalites[totalites %!in% globalitems]
# globalitems[globalitems %!in% totalites]
# toname<- totaltrade[measuredItemCPC %in% totalites[totalites %!in% globalitems],  ]
# tonanme <- nameData(domain = "trade", dataset = "total_trade_cpc_m49", toname, except = "timePointYears")
# write.xlsx(tonanme, 'missingitems.xlsx')

global_nct[, c('geographicAreaM49', 'timePointYearsSP'):= NULL]
global_nct_dcast <- dcast.data.table(global_nct, measuredItemCPC
                               ~ measuredElement, value.var = c('Value'))
completetrade_pop_nut <- merge(completetrade_pop, global_nct_dcast, by= 'measuredItemCPC', all.x = TRUE)
calculate_stat <- copy(completetrade_pop_nut)

########### IMPORT ################

# 1066	Energy [kcal/100g EP]
# 1061	Edible portion
# 1089	Water [g/100g EP]
# 1079	Protein [g/100g EP]
# 1067	Fat [g/100g EP]
# 1064	Carbohydrate, available [g/100g EP]
# 1068	Dietary fibre [g/100g EP]
# 1062	Alcohol [g/100g EP]
# 1063	Ash [g/100g EP]
# 1070	Calcium [mg/100g EP]
# 1071	Iron [mg/100g EP]
# 1072	Magnesium [mg/100g EP]
# 1073	Phosphorus [mg/100g EP]
# 1074	Potassium [mg/100g EP]
# 1075	Sodium [mg/100g EP]
# 1076	Zinc [mg/100g EP]
# 1083	Vitamin A [mcg RE/100g EP]
# 1084	Vitamin A [mcg RAE/100g EP]
# 1081	Thiamin [mg/100g EP]
# 1080	Riboflavin [mg/100g EP]
# 1078	Niacin [mg/100g EP]
# 1087	Vitamin C [mg/100g EP]
calculate_stat[,  qty_edible_import:= (`5610`*`1061`)]
# MACRO NUTRIENTS
calculate_stat[, `50002`:= (qty_edible_import*`1066`)/Population/365*10]
calculate_stat[, `50003`:= (qty_edible_import*`1079`)/Population/365*10]
calculate_stat[, `50004`:= (qty_edible_import*`1067`)/Population/365*10]
calculate_stat[, `50005`:= (qty_edible_import*`1064`)/Population/365*10]
calculate_stat[, `50006`:= (qty_edible_import*`1068`)/Population/365*10]

calculate_stat[, `50029`:= (qty_edible_import*`1089`)/Population/365*10]
calculate_stat[, `50007`:= (qty_edible_import*`1062`)/Population/365*10]
calculate_stat[, `50030`:= (qty_edible_import*`1063`)/Population/365*10]

# MICRO NUTRIENTS
calculate_stat[, `50008`:= (qty_edible_import*`1070`)/Population/365*10]
calculate_stat[, `50009`:= (qty_edible_import*`1071`)/Population/365*10]
calculate_stat[, `50010`:= (qty_edible_import*`1072`)/Population/365*10]
calculate_stat[, `50011`:= (qty_edible_import*`1073`)/Population/365*10]
calculate_stat[, `50012`:= (qty_edible_import*`1074`)/Population/365*10]
calculate_stat[, `50013`:= (qty_edible_import*`1075`)/Population/365*10]
calculate_stat[, `50014`:= (qty_edible_import*`1076`)/Population/365*10]

# VITAMINS
calculate_stat[, `50016`:= (qty_edible_import*`1083`)/Population/365*10]
calculate_stat[, `50017`:= (qty_edible_import*`1084`)/Population/365*10]
calculate_stat[, `50020`:= (qty_edible_import*`1081`)/Population/365*10]
calculate_stat[, `50021`:= (qty_edible_import*`1080`)/Population/365*10]
calculate_stat[, `50022`:= (qty_edible_import*`1078`)/Population/365*10]
calculate_stat[, `50028`:= (qty_edible_import*`1087`)/Population/365*10]



######### EXPORT ##########################


calculate_stat[,  qty_edible_export:= (`5910`*`1061`)]
# MACRO NUTRIENTS
calculate_stat[, `66002`:= (qty_edible_export*`1066`)/Population/365*10]
calculate_stat[, `66003`:= (qty_edible_export*`1079`)/Population/365*10]
calculate_stat[, `66004`:= (qty_edible_export*`1067`)/Population/365*10]
calculate_stat[, `66005`:= (qty_edible_export*`1064`)/Population/365*10]
calculate_stat[, `66006`:= (qty_edible_export*`1068`)/Population/365*10]

calculate_stat[, `66029`:= (qty_edible_export*`1089`)/Population/365*10]
calculate_stat[, `66007`:= (qty_edible_export*`1062`)/Population/365*10]
calculate_stat[, `66030`:= (qty_edible_export*`1063`)/Population/365*10]

# MICRO NUTRIENTS
calculate_stat[, `66008`:= (qty_edible_export*`1070`)/Population/365*10]
calculate_stat[, `66009`:= (qty_edible_export*`1071`)/Population/365*10]
calculate_stat[, `66010`:= (qty_edible_export*`1072`)/Population/365*10]
calculate_stat[, `66011`:= (qty_edible_export*`1073`)/Population/365*10]
calculate_stat[, `66012`:= (qty_edible_export*`1074`)/Population/365*10]
calculate_stat[, `66013`:= (qty_edible_export*`1075`)/Population/365*10]
calculate_stat[, `66014`:= (qty_edible_export*`1076`)/Population/365*10]

# VITAMINS
calculate_stat[, `66016`:= (qty_edible_export*`1083`)/Population/365*10]
calculate_stat[, `66017`:= (qty_edible_export*`1084`)/Population/365*10]
calculate_stat[, `66020`:= (qty_edible_export*`1081`)/Population/365*10]
calculate_stat[, `66021`:= (qty_edible_export*`1080`)/Population/365*10]
calculate_stat[, `66022`:= (qty_edible_export*`1078`)/Population/365*10]
calculate_stat[, `66028`:= (qty_edible_export*`1087`)/Population/365*10]

calculate_stat[,  c("5610", "5910", "Population", "1061", "1062", "1063", "1064", "1066", "1067",
                    "1068", "1070", "1071", "1072", "1073", "1074","1075", "1076", "1078", "1079", "1080", "1081",
                    "1083", "1084", "1087", "1089", "qty_edible_import", "qty_edible_export"):= NULL ]


calculate_stat2 <- melt.data.table(calculate_stat, id.vars = c("geographicAreaM49Reporter", "geographicAreaM49Partner", "measuredItemCPC","timePointYears"),
                measure.vars= c(names(calculate_stat)[names(calculate_stat) %!in% c("geographicAreaM49Reporter", "geographicAreaM49Partner", "measuredItemCPC","timePointYears")]),
                variable.name = "measuredElementTrade", value.name= "Value")


calculate_stat2 <- calculate_stat2[!is.na(Value),]


setcolorder(calculate_stat2, c("geographicAreaM49Reporter", "geographicAreaM49Partner", "measuredItemCPC", "measuredElementTrade", "timePointYears", "Value"))

calculate_stat2[, flagObservationStatus:= 'E']
calculate_stat2[, flagMethod:= 'e']

calculate_stat2$measuredElementTrade <- as.character(calculate_stat2$measuredElementTrade)

# sessionKey = swsContext.datasets[[1]]
# datasetConfig = GetDatasetConfig(domainCode = sessionKey@domain,
#                                  datasetCode = sessionKey@dataset)


SaveData(domain = "trade",
         dataset = "completed_tf_cpc_m49",
         data = calculate_stat2, waitTimeout = 2000000)








