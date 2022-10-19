##'
##' **Author: Aydan Selek**
##'
##' **Description:**
##'
##' This plugin updates the global unit values table year based.
##'
##'
##' **Inputs:**
##'
##' * total trade data
##'
##' **Flag assignment:**
##'
##' None


message("global UV update plugin starts...")

library(data.table)
library(faoswsTrade)
library(faosws)
library(stringr)
library(scales)
library(faoswsUtil)
library(faoswsFlag)
library(tidyr)
library(dplyr, warn.conflicts = FALSE)

`%!in%` = Negate(`%in%`)

if (CheckDebug()) {
  library(faoswsModules)
  SETTINGS = ReadSettings("modules/global_UVs_update/sws.yml")
  ## Define where your certificates are stored
  faosws::SetClientFiles(SETTINGS[["certdir"]])
  ## Get session information from SWS. Token must be obtained from web interface
  GetTestEnvironment(baseUrl = SETTINGS[["server"]],
                     token = SETTINGS[["token"]])
}

min_year = as.numeric(swsContext.computationParams$min_year)
max_year = as.numeric(swsContext.computationParams$max_year)
yearVals = min_year:max_year


# current_uv_list <- ReadDatatable('ess_trade_global_unit_values')
# unique(current_uv_list$region)

country_map <- ReadDatatable('continent_country_mapping_for_total_trade_tables')
country_map <- setDT(country_map)
eu_contries <- country_map$m49_country_code[duplicated(country_map$m49_country_code)] # Seperate EU countries from Europe
country_map <- country_map[!(country_group=='Europe' & m49_country_code %in% eu_contries),]
country_map <- country_map[!(m49_country_code== '196' & country_group== 'Asia'),] # Seperate also Cyprus from Asia


## Retrieve Total Trade data ##
geoKeys = GetCodeList(domain = "trade", dataset = "total_trade_cpc_m49",
                      dimension = "geographicAreaM49")[type == "country", code]

geoDim = Dimension(name = "geographicAreaM49", keys = geoKeys)

itemKeys = GetCodeList(domain = "trade", dataset = "total_trade_cpc_m49", "measuredItemCPC")
itemKeys = itemKeys[, code]
itemDim <- Dimension(name = "measuredItemCPC", keys = itemKeys)

eleDim <- c('5607', '5608', '5609', '5610', '5907', '5908', '5909', '5910', '5622', '5922',
  '5630', '5930', '5638', '5938', '5639', '5939', '5637', '5937') %>% Dimension(name = "measuredElementTrade", keys = .)

timeDim <- Dimension(name = "timePointYears", keys = as.character(yearVals))

#Define the key to pull SUA data
key = DatasetKey(domain = "trade", dataset = "total_trade_cpc_m49", dimensions = list(
  geographicAreaM49 = geoDim,
  measuredElementTrade = eleDim,
  measuredItemCPC = itemDim,
  timePointYears = timeDim
))

data = GetData(key,omitna = FALSE, normalized = FALSE)
data = normalise(data, areaVar = "geographicAreaM49",
                 itemVar = "measuredItemCPC", elementVar = "measuredElementTrade",
                 yearVar = "timePointYears", flagObsVar = "flagObservationStatus",
                 flagMethodVar = "flagMethod", valueVar = "Value",
                 removeNonExistingRecords = F)

trade <- nameData(domain = "trade", dataset = "total_trade_cpc_m49", data, except = "timePointYears")
trade <- trade[order(measuredItemCPC, measuredItemCPC_description, measuredElementTrade, timePointYears)]


trade <- merge(trade, country_map, by.x = 'geographicAreaM49', by.y = 'm49_country_code')

## heads, 1000 heads and hives will be accepted as quantity. Clean the metric tons for those ##
small_animals <- unique(trade[measuredElementTrade=='5609', measuredItemCPC])
big_animals <- unique(trade[measuredElementTrade=='5608', measuredItemCPC])
bees <-  unique(trade[measuredElementTrade=='5607', measuredItemCPC])
trade <- trade[!(measuredItemCPC %in% big_animals & measuredElementTrade %in% c('5610', '5910'))]
trade <- trade[!(measuredItemCPC %in% small_animals & measuredElementTrade %in% c('5610', '5910'))]
trade <- trade[!(measuredItemCPC %in% bees & measuredElementTrade %in% c('5610', '5910'))]
# Clean the unnecessary elements
trade <- trade[!(measuredElementTrade %in% c('5630', '5930', '5638', '5938', '5639', '5939', '5637', '5937'))]

trade[, flow := substr(measuredElementTrade, 1, 2)]
trade <- trade[, flow:=ifelse(flow=='56', 'import', 'export')]

trade <- trade[, c("flagObservationStatus", "flagMethod"):= NULL]
trade <- trade[!(measuredElementTrade == '5908' & measuredItemCPC == '2131')] # Mongolia issue has one Crop item saved as horses!!! Carefull!!!

trade_quantity <- trade[measuredElementTrade %!in% c('5622', '5922')]
trade_value <- trade[measuredElementTrade %in% c('5622', '5922')]


trade_quantity2 <- trade_quantity[, .(quantity=sum(Value, na.rm=TRUE)), by = c("measuredItemCPC", "measuredItemCPC_description", "flow", "timePointYears", "country_group")]
trade_quantity2_world <- trade_quantity[, .(quantity=sum(Value, na.rm=TRUE)), by = c("measuredItemCPC", "measuredItemCPC_description", "flow", "timePointYears")]
trade_quantity2_world[,country_group:=NA]
trade_quantity_all <- rbind(trade_quantity2, trade_quantity2_world)
trade_quantity_all <- trade_quantity_all[is.na(country_group), country_group:= 'World']

trade_value2 <- trade_value[, .(value=sum(Value, na.rm=TRUE)), by = c("measuredItemCPC", "measuredItemCPC_description", "flow", "timePointYears", "country_group")]
trade_value2_world <- trade_value[, .(value=sum(Value, na.rm=TRUE)), by = c("measuredItemCPC", "measuredItemCPC_description", "flow", "timePointYears")]
trade_value2_world[,country_group:=NA]
trade_value_all <- rbind(trade_value2, trade_value2_world)
trade_value_all <- trade_value_all[is.na(country_group), country_group:= 'World']


trade_q_v <- merge(trade_quantity_all, trade_value_all, by = c("measuredItemCPC", "measuredItemCPC_description",
                                                               "flow", "timePointYears", "country_group"))

trade_q_v <- trade_q_v[, unit_value:= value/quantity*1000]

setnames(trade_q_v, 'country_group', 'region')
trade_q_v <- trade_q_v[, c("quantity", "value"):= NULL]
trade_q_v <- trade_q_v[order(measuredItemCPC, measuredItemCPC_description, region,flow, timePointYears)]

trade_q_v[,
          `:=`(
            variation_unit_value = unit_value / shift(unit_value) - 1
          ),
          by = c("measuredItemCPC", "region", "flow")
          ]
trade_q_v <- trade_q_v[is.infinite(variation_unit_value), variation_unit_value:=0]
trade_q_v <- trade_q_v[is.infinite(unit_value), unit_value:=0]

setnames(trade_q_v, c("measuredItemCPC", "measuredItemCPC_description", "timePointYears"),
         c("measured_item_cpc", "measured_item_cpc_description", "time_point_years"))

message("Starting to save datatable on SWS...")

allPPRtables <- c("ess_trade_global_unit_values")

files <- list(trade_q_v)

for (i in 1:1) {

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

