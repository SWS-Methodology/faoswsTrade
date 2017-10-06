setwd('C:/Users/mongeau/Dropbox/GitHub/SWS-Methodology/faoswsTrade')


# Parameters

# For parallel computation
multicore <- TRUE
# Maximum allowed discrepancy in the flow/mirror ratio
# TODO: should be a parameter
threshold <- 0.5
# Years
years <- as.character(2000:2015)
# movav order
movav_order <- 3

library(faosws)
library(dplyr)
#library(tidyr)
# stringr, zoo

# elements (Q = Quantity, UV = Unit Value):
# 5.00, Q, kg
# 5.01, Q, l
# 5.07, Q, #
# 5.08, Q, head
# 5.09, Q, 1000 head
# 5.10, Q, t
# 5.11, Q, 1000 t
# 5.15, Q, <BLANK>
# 5.16, Q, m3
# 5.17, Q, PJ
# 5.30, UV, $/t
# 5.36, UV, $/m3
# 5.37, UV, $/Unit
# 5.38, UV, $/head
# 5.39, UV, $/1000 head

if (CheckDebug()) {
  library(faoswsModules)
  settings_file <- "modules/timeseries_complete_tf_cpc/sws.yml"
  SETTINGS = faoswsModules::ReadSettings(settings_file)

  ## Define where your certificates are stored
  SetClientFiles(SETTINGS[["certdir"]])

  ## Get session information from SWS.
  ## Token must be obtained from web interface
  GetTestEnvironment(baseUrl = SETTINGS[["server"]],
                     token   = SETTINGS[["token"]])
}

#GetCodeList('trade', 'completed_tf_cpc_m49', 'measuredItemCPC')
#GetDatasetConfig('trade', 'completed_tf_cpc_m49')

GetCodeList2 <- function(dimension = NA) {
  GetCodeList(
    domain    = 'trade',
    dataset   = 'completed_tf_cpc_m49',
    dimension = dimension
  )
}

Vars <- list(
  reporters = 'geographicAreaM49Reporter',
  partners  = 'geographicAreaM49Partner',
  items     = 'measuredItemCPC',
  elements  = 'measuredElementTrade',
  years     = 'timePointYears'
)

reporters <- GetCodeList2(dimension = Vars[['reporters']])[type == 'country', code]

Keys <- list(
  #reporters = reporter,
  partners = GetCodeList2(dimension = Vars[['partners']])[type == 'country', code],
  items    = GetCodeList2(dimension = Vars[['items']])[, code],
  # Quantity [#], Quantity [head], Quantity [1000 head], Quantity [t], Value [1000 $]
  elements = c('5607', '5608', '5609', '5610', '5622',
               '5907', '5908', '5909', '5910', '5922'),
  years    = years
)


######################################################################
for ( i in  dir("R/", full.names = TRUE) ) source(i)
######################################################################

calculateOnDataSWS <- function(data = NA) {
  
  # Removing dplyr:: has unintended consequences

  # XXX this should be the mimimum, but it's not necessarily true.
  if (nrow(data) > 10) {
    ## Number of yearly points by commodity
    #plot(x %>% group_by(geographicAreaM49Partner, measuredItemCPC) %>% dplyr::count() %$% table(n))

    N <- length(unique(data$timePointYears))
    morder <- ifelse(N <= movav_order, N, movav_order)

    tradedata <- data %>%
      reshapeTrade() %>%
      # XXX should be "value only" items
      dplyr::filter(!is.na(qty)) %>%
      dplyr::mutate(uv = value / qty) %>%
      tidyr::complete(
        tidyr::nesting(
          flow,
          geographicAreaM49Reporter,
          geographicAreaM49Partner,
          measuredItemCPC
        ),
        timePointYears
      ) %>%
      dplyr::arrange(
        flow,
        geographicAreaM49Reporter,
        geographicAreaM49Partner,
        measuredItemCPC,
        dplyr::desc(timePointYears)
      ) %>%
      dplyr::group_by(
        flow,
        geographicAreaM49Reporter,
        geographicAreaM49Partner,
        measuredItemCPC
      ) %>%
      dplyr::mutate(
        ma_v_fwd = movav(value, pkg = 'zoo', mode = 'lag', n = N),
        ma_q_fwd = movav(qty, pkg = 'zoo', mode = 'lag', n = N),
        ma_fwd   = ma_v_fwd / ma_q_fwd
      ) %>%
      dplyr::arrange(
        flow,
        geographicAreaM49Reporter,
        geographicAreaM49Partner,
        measuredItemCPC,
        timePointYears
      ) %>%
      dplyr::group_by(
        flow,
        geographicAreaM49Reporter,
        geographicAreaM49Partner,
        measuredItemCPC
      ) %>%
      #mutate(var = uv/lag(uv)-1)
      dplyr::mutate(
        ma_v_bck = movav(value, pkg = 'zoo', mode = 'lag', n = N),
        ma_q_bck = movav(qty, pkg = 'zoo', mode = 'lag', n = N),
        ma_bck   = ma_v_bck / ma_q_bck,
        ma       = ifelse(
                     timePointYears %in% years[1:N],
                     ma_fwd,
                     ma_bck
                   ),
        ratio_ma = uv/ma,
        nna_ma   = sum(!is.na(ma)),
        out      = if_else(
                     between(
                       ratio_ma,
                       1-threshold,
                       1+threshold),
                     1,
                     0
                   ),
        qty_new  = if_else(out == 1, value/ma, qty),
        uv_new   = value/qty_new
      ) %>%
      #dplyr::filter(nna_ma > 0)
      dplyr::ungroup()
  
    invisible(gc())

    return(tradedata)

  } else {
    NULL
  }
}

computeData <- function(reporter = NA) {
  getComputedDataSWS(reporter) %>%
  calculateOnDataSWS()
}


if (multicore) {
  n_cores <- parallel::detectCores() - 1
  cl <- parallel::makeCluster(n_cores)
  doParallel::registerDoParallel(cl)

  parallel::clusterEvalQ(cl, {
                         library(faosws)
                         library(dplyr)
                         NULL
               })

  parallel::clusterExport(cl,
    c(
      'SETTINGS',
      ls(pattern = 'swsContext'),
      'getComputedDataSWS',
      'calculateOnDataSWS',
      'computeData',
      'years',
      'Vars',
      'Keys',
      'reshapeTrade',
      'movav',
      'movav_order',
      'threshold'
    )
  )

  if(CheckDebug()) {
    parallel::clusterEvalQ(cl,
      {
        ## Define where your certificates are stored
        faosws::SetClientFiles(SETTINGS[["certdir"]])
        NULL
      }
    )
  }

}


start_time <- Sys.time()
db_list <- plyr::mlply(
  #expand.grid(
  #  reporters,
  #  stringsAsFactors = FALSE
  #  ) %>%
  #    as_data_frame() %>%
  #    dplyr::rename(reporter = Var1, year = Var2),
  data_frame(reporter = reporters),
  .fun = computeData,
  .parallel = multicore,
  .progress = 'text'
)
end_time <- Sys.time()
print(end_time-start_time)

invisible(gc())

names(db_list) <- reporters

db_list <- db_list[!sapply(db_list, is.null)]

new_reporters <- names(db_list)

countries_no_data <- setdiff(reporters, new_reporters)




db_final <- list()

db_stats <- data.frame(
  reporter         = new_reporters,
  n                = NA_real_,
  n_m              = NA_real_,
  n_x              = NA_real_,
  n_out            = NA_real_,
  n_out_m          = NA_real_,
  n_out_x          = NA_real_,
  perc_out         = NA_real_,
  perc_out_m       = NA_real_,
  perc_out_x       = NA_real_,
  stringsAsFactors = FALSE
)

start_time <- Sys.time()
p <- progress_estimated(length(new_reporters))
for (reporter in new_reporters) {

  p$tick()$print()

  db_tmp <- db_list[[reporter]] %>%
  dplyr::filter(!is.na(qty) | !is.na(value)) %>%
  select(
    geographicAreaM49Reporter,
    geographicAreaM49Partner,
    flow,
    measuredItemCPC,
    timePointYears,
    flag_qty,
    flag_value,
    qty = qty_new,
    value,
    uv = uv_new,
    out,
    measuredElementTrade_q
  )

  mystats <- c(
    nrow(db_tmp),
    nrow(db_tmp[db_tmp$flow == 1,]),
    nrow(db_tmp[db_tmp$flow == 2,]),
    sum(db_tmp$out),
    sum(db_tmp[db_tmp$flow == 1,]$out),
    sum(db_tmp[db_tmp$flow == 2,]$out),
    sum(db_tmp$out)/nrow(db_tmp),
    sum(db_tmp[db_tmp$flow == 1,]$out)/nrow(db_tmp[db_tmp$flow == 1,]),
    sum(db_tmp[db_tmp$flow == 2,]$out)/nrow(db_tmp[db_tmp$flow == 2,])
  )

  db_stats[db_stats$reporter == reporter, -1] <- mystats

  db_uv <- db_tmp %>%
    select(-flag_value, -qty, -value, -out) %>%
    # The UV flag is the weakest, i.e., the qty one.
    tidyr::separate(
      flag_qty,
      sep    = '-',
      into   = c('flagObservationStatus', 'flagMethod'),
      remove = TRUE
    ) %>%
    dplyr::mutate(flagMethod = 'i') %>%
    dplyr::rename(Value = uv) %>%
    # As on 2017-03-20 this works (see the element list at the beginning),
    # but it's not probably the best way to do it.
    dplyr::mutate(
      measuredElementTrade = stringr::str_replace(
                                                  measuredElementTrade_q,
                                                  '^(..).(.)$',
                                                  '\\13\\2')
    ) %>%
    select(
      geographicAreaM49Reporter,
      geographicAreaM49Partner,
      measuredElementTrade,
      measuredItemCPC,
      timePointYears,
      Value,
      flagObservationStatus,
      flagMethod
    )



  db_q_v <- db_tmp %>%
    select(-uv) %>%
    tidyr::gather(type, Value, qty, value) %>%
    dplyr::mutate(flag = ifelse(type == 'qty', flag_qty, flag_value)) %>%
    select(-flag_qty, -flag_value) %>%
    tidyr::separate(
      flag,
      sep    = '-',
      into   = c('flagObservationStatus', 'flagMethod'),
      remove = TRUE
    ) %>%
    dplyr::mutate(
      measuredElementTrade = ifelse(
                                    type != 'value',
                                    measuredElementTrade_q,
                                    ifelse(flow == 1, '5622', '5922')
                                    )
    ) %>%
    dplyr::mutate(
      flagObservationStatus = ifelse(
                                     type == 'qty' & out == 1,
                                     'I',
                                     flagObservationStatus
                                     ),
      flagMethod = ifelse(
                          type == 'qty' & out == 1,
                          'e',
                          flagMethod
                          )
    ) %>%
    #select(-flow, -measuredElementTrade_q, -type, -out)
    select(
      geographicAreaM49Reporter,
      geographicAreaM49Partner,
      measuredElementTrade,
      measuredItemCPC,
      timePointYears,
      Value,
      flagObservationStatus,
      flagMethod
    )

  db_final[[reporter]] <- bind_rows(db_q_v, db_uv) %>%
    data.table::as.data.table()

}
end_time <- Sys.time()
print(end_time-start_time)










for (reporter in new_reporters) {
  message(sprintf("Writing data to session/database, country %s", reporter))
  
  stats <- SaveData("trade",
                    "total_trade_cpc_m49",
                    db_final[[reporter]],
                    waitTimeout = 10800)

  sprintf(
    "Country: %s
    Values inserted: %s
    appended: %s
    ignored: %s
    discarded: %s",
    reporter,
    stats[["inserted"]],
    stats[["appended"]],
    stats[["ignored"]],
    stats[["discarded"]]
  )

}


