#setwd('C:/Users/mongeau/tmp/faoswsTrade')

######################################################################
for ( i in  dir("R/", full.names = TRUE) ) source(i)
######################################################################


# Parameters

# For parallel computation
multicore <- TRUE

# Maximum allowed discrepancy in the flow/mirror ratio
# TODO: should be a parameter
threshold <- as.numeric(swsContext.computationParams$threshold)

# Years
# TODO: use range: disabled because the TODO in accuracyScores (see below)
years <- swsContext.computationParams$year
#years <- swsContext.computationParams$startyear:swsContext.computationParams$endyear

# Whether to smooth trade or not.
# XXX TRUE is being under development.
smooth_trade <- FALSE

library(faosws)
library(dplyr)
#library(tidyr)
# igraph, stringr, reshape2

if (CheckDebug()) {
  library(faoswsModules)
  settings_file <- "modules/balance_complete_tf_cpc/sws.yml"
  SETTINGS = faoswsModules::ReadSettings(settings_file)

  ## Define where your certificates are stored
  SetClientFiles(SETTINGS[["certdir"]])

  ## Get session information from SWS.
  ## Token must be obtained from web interface
  GetTestEnvironment(baseUrl = SETTINGS[["server"]],
                     token = SETTINGS[["token"]])

  dir_to_save <- paste0(Sys.getenv('HOME'), '/tmp/')
} else {
  dir_to_save <- '/work/SWS_R_Share/trade/validation_tool_files/tmp/'
}

name_to_save <- 'db_balance.rds'

#GetCodeList('trade', 'completed_tf_cpc_m49', 'measuredItemCPC')
#GetDatasetConfig('trade', 'completed_tf_cpc_m49')

GetCodeList2 <- function(dimension = NA) {
  GetCodeList(
              domain = 'trade',
              dataset = 'completed_tf_cpc_m49',
              dimension = dimension
              )
}

## ???
#smoothTrade <- function(data = NA) {
#  data %>%
#    group_by(reporter, partner, flow, cpc) %>%
#    dplyr::arrange(reporter, partner, flow, cpc, year) %>%
#    dplyr::mutate(
#           qty_movav    = movav(qty),
#           qty_m_movav  = movav(qty_m),
#           ratio_mirror = qty_movav/qty_m_movav
#           ) %>%
#    ungroup() %>%
#    dplyr::select(-qty_movav, -qty_m_movav)
#}



#expandData <- function(data = NA) {
#  reshape2::melt(data,
#                 id = c('geographicAreaM49Reporter',
#                        'geographicAreaM49Partner',
#                        'measuredElementTrade',
#                        'measuredItemCPC',
#                        'timePointYears',
#                        'flagObservationStatus',
#                        'flagMethod'),
#                   value.name = 'Value') %>%
#  tbl_df() %>%
#  dplyr::select(-variable, -L1) %>%
#  tidyr::complete(
#           tidyr::nesting(
#                   geographicAreaM49Reporter,
#                   geographicAreaM49Partner,
#                   measuredElementTrade,
#                   measuredItemCPC
#                   ),
#           timePointYears = as.character(years)
#           ) %>%
#  dplyr::arrange(
#          geographicAreaM49Reporter,
#          geographicAreaM49Partner,
#          measuredElementTrade,
#          measuredItemCPC,
#          timePointYears
#          )
#}

Vars <- list(reporters = 'geographicAreaM49Reporter',
             partners  = 'geographicAreaM49Partner',
             items     = 'measuredItemCPC',
             elements  = 'measuredElementTrade',
             years     = 'timePointYears')


reporters <- GetCodeList2(dimension = Vars[['reporters']])[type == 'country', code]


Keys <- list( #reporters = rep,
             partners = GetCodeList2(dimension = Vars[['partners']])[type == 'country', code],
             items    = GetCodeList2(dimension = Vars[['items']])[, code],
             # Quantity [#], Quantity [head], Quantity [1000 head], Quantity [t], Value [1000 $]
             elements = c('5607', '5608', '5609', '5610', '5622',
                          '5907', '5908', '5909', '5910', '5922'),
             years    = as.character(years)
             )

if (multicore) {
  n_cores <- parallel::detectCores() - 1
  cl <- parallel::makeCluster(n_cores)
  doParallel::registerDoParallel(cl)

  parallel::clusterExport(cl, c(ls(pattern = 'swsContext')))

  if(CheckDebug()) {
    parallel::clusterExport(cl, 'SETTINGS')

    parallel::clusterEvalQ(cl, {
      ## Define where your certificates are stored
      faosws::SetClientFiles(SETTINGS[["certdir"]])
               })
  }

  parallel::clusterExport(cl, c('getComputedDataSWS', 'Vars', 'Keys'))
  parallel::clusterEvalQ(cl, library(faosws))
}



start_time <- Sys.time()
db_list <- plyr::mlply(
                       #expand.grid(
                       #reporters,
                       #stringsAsFactors = FALSE
                       #) %>%
                       #  as_data_frame() %>%
                       #  dplyr::rename(reporter = Var1, year = Var2),
                       reporters,
                       .fun = function(x) getComputedDataSWS(x, omit = TRUE),
                       .parallel = multicore,
                       .progress = 'text'
                       )
end_time <- Sys.time()
print(end_time-start_time)


#####################################
# XXX what happens with "value only'?
#####################################

tradedata <- db_list %>%
  meltTradeData()

cpc_units <- tradedata %>%
  dplyr::select(measuredElementTrade, measuredItemCPC) %>%
  dplyr::mutate(
         measuredElementTrade = stringr::str_sub(measuredElementTrade, 3, 4)
         ) %>%
  distinct() %>%
  dplyr::filter(measuredElementTrade != '22')

# Remove livestock weights: some livestocks have also the weight
# in kilograms, but the final unit is head or 1000 heads, so we
# remove the weight (below)
livestock_weights <-
  cpc_units %>%
  group_by(measuredItemCPC) %>%
  dplyr::mutate(n = n()) %>%
  ungroup() %>%
  dplyr::filter(n == 2 & measuredElementTrade == 10) %>%
  dplyr::select(-n)

tradedata <-
  anti_join(
    tradedata,
    left_join(
      livestock_weights,
      data.frame(measuredElementTrade = '10', flow = c('56', '59'), stringsAsFactors = FALSE),
      by = 'measuredElementTrade') %>%
      dplyr::mutate(measuredElementTrade = paste0(flow, measuredElementTrade)) %>%
      dplyr::select(-flow),
    by = c('measuredElementTrade', 'measuredItemCPC')
  )

#rm(db_list)
#invisible(gc())

tradedata <- tradedata %>%
  reshapeTrade() %>%
  addMirror()

invisible(gc())

# If smooth_trade == TRUE it means that we will use moving averages of
# the mirror ratio, thus we need to expand the data set for all years.
if (smooth_trade) {
  tradedata <- tradedata %>%
    dplyr::mutate(orig = 1)

  Sys.time()
  a <- tradedata %>%
    tidyr::complete(
             tidyr::nesting(
                     geographicAreaM49Reporter,
                     geographicAreaM49Partner,
                     measuredItemCPC,
                     flow
                     ),
             timePointYears
             ) # almost 8 mins
  Sys.time()

  invisible(gc())

  Sys.time()
  a1 <- a %>%
    group_by(geographicAreaM49Reporter, geographicAreaM49Partner, measuredItemCPC,  flow) %>%
    dplyr::mutate(ratio_mirr_movav = RcppRoll::roll_mean(ratio_mirror, n = 3, fill = NA, na.rm = TRUE)) %>%
    ungroup() %>%
    dplyr::filter(orig == 1) %>%
    dplyr::select(-orig) %>%
    dplyr::rename(ratio_mirror_orig = ratio_mirror, ratio_mirror = ratio_mirr_movav)
  Sys.time()
}

tradedata <- tradedata %>%
  dplyr::mutate(discrep_mirr = !between(ratio_mirror, 1/threshold, threshold))

# Generate accuracy scores for each country
# TODO: generate scores by year
accu_table <- accuracyScores(data = tradedata)

tradedata <- left_join(tradedata, accu_table, by = c('geographicAreaM49Reporter' = 'country')) %>%
  left_join(accu_table, by = c('geographicAreaM49Partner' = 'country'), suffix = c('_r', '_m')) %>%
  dplyr::rename(
         accu_score = accu_score_r,
         accu_rank  = accu_rank_r,
         accu_group = accu_group_r
         ) %>%
  # DEBUG
  dplyr::mutate(
         orig_qty        = qty,
         orig_value      = value,
         orig_flag_qty   = flag_qty,
         orig_flag_value = flag_value
         )

fclunits <- ReadDatatable('fclunits')

value_only_items <-
  fclunits[fcu_fclunit == '$ value only']$fcu_fcl %>%
  faoswsUtil::fcl2cpc()

# XXX Notice that we are re-calculating `discrep_mirr`
# By using the ratio of values. This should be done before.
tradedata_value_only <-
  tradedata %>%
  dplyr::filter(measuredItemCPC %in% value_only_items) %>%
  dplyr::mutate(discrep_mirr = !between(value/value_m, 1/threshold, threshold)) %>%
  dplyr::mutate(mirror_results = mirrorRules(., variable = 'value')) %>%
  dplyr::mutate(
    value      = ifelse(grepl('prt', mirror_results), value_m, value),
    flag_value = ifelse(grepl('prt', mirror_results), 'T-i', flag_value)
  )


tradedata_value_and_qty <-
  tradedata %>%
  dplyr::filter(!(measuredItemCPC %in% value_only_items)) %>%
  dplyr::mutate(mirror_results = mirrorRules(., variable = 'qty')) %>%
  dplyr::mutate(
    qty        = ifelse(grepl('prt', mirror_results), qty_m, qty),
    flag_qty   = ifelse(grepl('prt', mirror_results), 'T-c', flag_qty),
    # XXX here, there's no need to copy a value if it's in the +/- 12%,
    # but for now let's copy it in any case.
    value      = ifelse(grepl('prt', mirror_results), value_m, value),
    flag_value = ifelse(grepl('prt', mirror_results), 'T-i', flag_value)
  )

# TODO: now, we can have transactions for which quantities are OK, but
# values are not. whatdo?
# Here's an attempt. Using again thresold. Also, re-calculating `discrep_mirr`,
# but see a previous comment.

cond_value_discrep <-
  with(
    tradedata_value_and_qty,
    !discrep_mirr &
    !is.na(value) &
    !is.na(value_m) &
    (value > threshold * value_m | value < 1/threshold * value_m)
  )

tradedata_value_and_qty_discrep <-
  tradedata_value_and_qty %>%
  dplyr::filter(cond_value_discrep) %>%
  dplyr::mutate(discrep_mirr = !between(value/value_m, 1/threshold, threshold)) %>%
  dplyr::mutate(mirror_results = mirrorRules(., variable = 'value')) %>%
  # Don't touch quantities, as they are not discrepant.
  dplyr::mutate(
    value      = ifelse(grepl('prt', mirror_results), value_m, value),
    flag_value = ifelse(grepl('prt', mirror_results), 'T-i', flag_value)
  )

tradedata_value_and_qty <-
  bind_rows(
    dplyr::filter(tradedata_value_and_qty, !cond_value_discrep),
    tradedata_value_and_qty_discrep
  )

tradedata_XXX <-
  bind_rows(
    tradedata_value_and_qty,
    tradedata_value_only
  )




# Let's now put all together.

tradedata_final <- tradedata %>%
  dplyr::select(
         geographicAreaM49Reporter,
         geographicAreaM49Partner,
         flow,
         measuredItemCPC,
         timePointYears,
         flag_qty,
         flag_value,
         qty,
         value
         ) %>%
  tidyr::gather(type, Value, qty, value) %>%
  dplyr::mutate(flag = ifelse(type == 'qty', flag_qty, flag_value)) %>%
  dplyr::select(-flag_qty, -flag_value) %>%
  tidyr::separate(
                  flag,
                  sep    = '-',
                  into   = c('flagObservationStatus', 'flagMethod'),
                  remove = TRUE
                  )


complete_trade_flow_cpc <-
  left_join(tradedata_final %>% ungroup(), cpc_units) %>%
  dplyr::mutate(
    measuredElementTrade =
      ifelse(
        type == 'value',
        '22',
        measuredElementTrade
      ),
    flow = ifelse(flow == 1, '56', '59')
  ) %>%
  dplyr::mutate(measuredElementTrade = paste0(flow, measuredElementTrade)) %>%
  dplyr::select(-flow, -type) %>%
  dplyr::filter(!(measuredElementTrade %in% c('56NA', '59NA'))) %>%
  dplyr::filter(!is.na(Value), !is.na(flagObservationStatus), !is.na(flagMethod))

complete_trade_flow_cpc <- data.table::as.data.table(complete_trade_flow_cpc)

data.table::setcolorder(complete_trade_flow_cpc,
                        c("geographicAreaM49Reporter",
                          "geographicAreaM49Partner",
                          "measuredElementTrade",
                          "measuredItemCPC",
                          "timePointYears",
                          "Value",
                          "flagObservationStatus",
                          "flagMethod"))

stats <- SaveData("trade",
                  "analytical_completed_tf_cpc_m49",
                  complete_trade_flow_cpc,
                  waitTimeout = 10800)

sprintf(
  "Module completed in %1.2f minutes.
  Values inserted: %s
  appended: %s
  ignored: %s
  discarded: %s",
  difftime(Sys.time(), startTime, units = "min"),
  stats[["inserted"]],
  stats[["appended"]],
  stats[["ignored"]],
  stats[["discarded"]]
)

