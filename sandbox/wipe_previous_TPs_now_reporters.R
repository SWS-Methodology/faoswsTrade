# Script to wipe all data from SWS for those countries that were
# previously computed as TP but for which now there is official data.

# Reporters to wipe from SWS (year specific)
REPORTERS <-
  tibble::tribble(
    ~reporter, ~year,
       44, 2014,
      364, 2014,
      296, 2014,
      422, 2014,
      496, 2014,
      800, 2014,
      894, 2014,
       44, 2015,
      270, 2015,
      340, 2015,
      296, 2015,
      422, 2015,
      800, 2015,
      894, 2015,
      108, 2016,
      120, 2016,
      384, 2016,
      320, 2016,
      364, 2016,
      422, 2016,
      454, 2016,
      466, 2016,
      524, 2016,
      558, 2016,
      800, 2016,
      854, 2017,
      320, 2017,
      454, 2017,
      704, 2017
    # HERE!
  )

# complete trade token:
TOKEN_COMPLETE <- "ca4736d0-debd-4f83-896e-29553fb858d1"

# total trade token:
TOKEN_TOTAL <- "83f9d748-ffe7-41c0-9802-68c32167d360"

############################################################################

library(data.table)
library(faosws)

mydir <- "c:/Users/mongeau.FAODOMAIN/Dropbox/GitHub/SWS-Methodology/faoswsTrade/modules/complete_tf_cpc"

SETTINGS <- faoswsModules::ReadSettings(file.path(mydir, "sws.yml"))

SetClientFiles(SETTINGS[["certdir"]])

codes_com <- function(dimension = NA) {
  GetCodeList(
    domain    = 'trade',
    dataset   = 'completed_tf_cpc_m49',
    dimension = dimension
  )
}

codes_tot <- function(dimension = NA) {
  GetCodeList(
    domain    = 'trade',
    dataset   = 'total_trade_cpc_m49',
    dimension = dimension
  )
}

######## Bilateral

GetTestEnvironment(baseUrl = SETTINGS[["server"]], token = TOKEN_COMPLETE)

D_comp <- list()

for (year in as.character(unique(REPORTERS$year))) {
  reporters <- as.character(REPORTERS[REPORTERS$year == year,]$reporter)

  Keys <- list(reporters = reporters,
               partners  = codes_com(dimension = 'geographicAreaM49Partner')[type == 'country', code],
               items     = codes_com(dimension = 'measuredItemCPC')[, code],
               elements  = codes_com(dimension = 'measuredElementTrade')[, code],
               years     = as.character(year))

  key <- DatasetKey(domain     = 'trade',
                    dataset    = 'completed_tf_cpc_m49',
                    dimensions = list(
                      Dimension(name = 'geographicAreaM49Reporter', keys = Keys[['reporters']]),
                      Dimension(name = 'geographicAreaM49Partner',  keys = Keys[['partners']]),
                      Dimension(name = 'measuredItemCPC',           keys = Keys[['items']]),
                      Dimension(name = 'measuredElementTrade',      keys = Keys[['elements']]),
                      Dimension(name = 'timePointYears',            keys = Keys[['years']])))

  D_comp[[year]] <- GetData(key = key, omitna = TRUE)
}

D_comp_save <- rbindlist(D_comp)

D_comp_save[, `:=`(Value = NA_real_, flagObservationStatus = NA_character_, flagMethod = NA_character_)]

stats_comp <- SaveData("trade", "completed_tf_cpc_m49", D_comp_save, waitTimeout = 10800)

######## Total

GetTestEnvironment(baseUrl = SETTINGS[["server"]], token = TOKEN_TOTAL)

D_tot <- list()

for (year in as.character(unique(REPORTERS$year))) {
  reporters <- as.character(REPORTERS[REPORTERS$year == year,]$reporter)

  Keys <- list(reporters = reporters,
               items     = codes_com(dimension = 'measuredItemCPC')[, code],
               elements  = codes_com(dimension = 'measuredElementTrade')[, code],
               years     = as.character(year))

  key <- DatasetKey(domain     = 'trade',
                    dataset    = 'total_trade_cpc_m49',
                    dimensions = list(
                      Dimension(name = 'geographicAreaM49', keys = Keys[['reporters']]),
                      Dimension(name = 'measuredItemCPC',           keys = Keys[['items']]),
                      Dimension(name = 'measuredElementTrade',      keys = Keys[['elements']]),
                      Dimension(name = 'timePointYears',            keys = Keys[['years']])))

  D_tot[[year]] <- GetData(key = key, omitna = TRUE)
}

D_tot_save <- rbindlist(D_tot)

D_tot_save[, `:=`(Value = NA_real_, flagObservationStatus = NA_character_, flagMethod = NA_character_)]

stats_tot <- SaveData("trade", "total_trade_cpc_m49", D_tot_save, waitTimeout = 10800)

