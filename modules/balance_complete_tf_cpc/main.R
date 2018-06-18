stopifnot(!is.null(swsContext.computationParams$startyear))
stopifnot(!is.null(swsContext.computationParams$endyear))
stopifnot(!is.null(swsContext.computationParams$threshold))
print(swsContext.computationParams$startyear)
print(swsContext.computationParams$endyear)
print(swsContext.computationParams$threshold)


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
years <- swsContext.computationParams$startyear:swsContext.computationParams$endyear

# Whether to smooth trade or not
smooth_trade <- TRUE

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
#    select(-qty_movav, -qty_m_movav)
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
#  select(-variable, -L1) %>%
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
                       data_frame(reporter = reporters),
                       .fun = getComputedDataSWS,
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
  select(measuredElementTrade, measuredItemCPC) %>%
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

rm(db_list)
invisible(gc())

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
    dplyr::mutate(ratio_mirr_movav = RcppRoll::roll_mean(ratio_mirror, n = 3, fill = NA, na.rm = TRUE))
  Sys.time()

  # after loading a1
  Sys.time()
  a2 <- a1 %>%
    dplyr::filter(orig == 1) %>%
    select(-orig) %>%
    dplyr::rename(ratio_mirror_orig = ratio_mirror, ratio_mirror = ratio_mirr_movav) %>%
    ungroup()
  Sys.time()
}

tradedata <- tradedata %>%
  dplyr::mutate(discrep_mirr = !between(ratio_mirror, 1/threshold, threshold))

# Generate accuracy scores for each country
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

mirror_results <- tradedata %>%
  mirrorRules(variable = 'qty')

tradedata_final <- tradedata %>%
  select(
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
  select(-flag_qty, -flag_value) %>%
  tidyr::separate(
                  flag,
                  sep    = '-',
                  into   = c('flagObservationStatus', 'flagMethod'),
                  remove = TRUE
                  )



final_db <- left_join(tradedata_final %>% ungroup(), cpc_units) %>%
dplyr::mutate(
       measuredElementTrade = ifelse(
                                      type == 'value',
                                      '22',
                                      measuredElementTrade
                                      ),
       flow = ifelse(flow == 1, '56', '59')
       ) %>%
dplyr::mutate(measuredElementTrade = paste0(flow, measuredElementTrade)) %>%
select(-flow, -type) %>%
dplyr::filter(!(measuredElementTrade %in% c('56NA', '59NA')))


saveRDS(final_db, file = paste0(dir_to_save, name_to_save))

