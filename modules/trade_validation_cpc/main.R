##' ---
##' title: "Appendix: `trade_validation_cpc` module"
##' author:
##'   - Christian A. Mongeau Ospina\
##'
##'     Food and Agriculture Organization of the United Nations
##' date: "`r format(Sys.time(), '%e %B %Y')`"
##' output:
##'    pdf_document
##' ---

##' This document shows the operations that the `trade_validation_cpc`
##' module carries out in order to build the dataset used in the
##' validation of trade flows.

##+ setup, include=FALSE
knitr::opts_chunk$set(echo = FALSE, eval = FALSE)

# For parallel computation
multicore <- TRUE
# Maximum allowed discrepancy in the flow/mirror ratio
# TODO: should be a parameter
threshold <- 0.5

library(faosws)
library(dplyr)
#library(tidyr)
# stringr, zoo, RcppRoll, robustbase

##+ sws

if (CheckDebug()) {
  library(faoswsModules)
  settings_file <- "modules/trade_validation_cpc/sws.yml"
  SETTINGS = faoswsModules::ReadSettings(settings_file)

  ## Define where your certificates are stored
  SetClientFiles(SETTINGS[["certdir"]])

  ## Get session information from SWS.
  ## Token must be obtained from web interface
  GetTestEnvironment(baseUrl = SETTINGS[["server"]],
                     token   = SETTINGS[["token"]])

  dir_to_save <- paste0(Sys.getenv('HOME'), '/validation_tool_files/')
} else {
  dir_to_save <- '/work/SWS_R_Share/trade/validation_tool_files/'
}

DB_rds_storage <- paste0(dir_to_save, 'tmp/DB_rds_storage/')
name_to_save <- 'db.rds'

print_log <- function(x) {
  print(paste('TradeValidation', format(Sys.time(), "%Y%m%d%H%M"), x))
}

##+ check_parameters

stopifnot(!is.null(swsContext.computationParams$startyear))
stopifnot(!is.null(swsContext.computationParams$endyear))
stopifnot(!is.null(swsContext.computationParams$useprevious))

##' # Parameters

##' - `startyear` (SWS label: "Start year"): first year for processing.
##' - `endyear` (SWS label: "End year"): last year for processing.
##' - `useprevious` (SWS label: "Use previous data?"): if set to `TRUE`,
##' previously downloaded data will be used, otherwise a new query will
##' be performed. *This should always be `FALSE`, leaving `TRUE` as a
##' viable option only for testing/debugging purposes.*
##' - `multicore`: if set to `TRUE`, operations will be performed with
##' parallel programming, i.e., sequential operations will be distributed
##' to the different cores of the machine on which the module runs. This
##' implies a reduction of computation time. *This is a hardcoded parameter.*
##' - `threshold`: threshold used to define what an outlier is (see below).
##' *This is a hardcoded parameter.*
##' - `morder`: order of the moving average of unit values. *This is a
##' hardcoded parameter.*

print_log('Parameters')
print(swsContext.computationParams$startyear)
print(swsContext.computationParams$endyear)
print(swsContext.computationParams$useprevious)

years <- swsContext.computationParams$startyear:swsContext.computationParams$endyear

# If TRUE, use previously downloaded files
# (useful mainly for testing)
use_previous <- as.logical(swsContext.computationParams$useprevious)

if (!file.exists(dir_to_save)) dir.create(dir_to_save, recursive = TRUE)
if (!file.exists(DB_rds_storage)) dir.create(DB_rds_storage, recursive = TRUE)

######################################################################
for ( i in  dir("R/", full.names = TRUE) ) source(i)
######################################################################


# elements (Q = Quantity, UV = Unit Value):
# 5.00,  Q, kg
# 5.01,  Q, l
# 5.07,  Q, #
# 5.08,  Q, head
# 5.09,  Q, 1000 head
# 5.10,  Q, t
# 5.11,  Q, 1000 t
# 5.15,  Q, <BLANK>
# 5.16,  Q, m3
# 5.17,  Q, PJ
# 5.30, UV, $/t
# 5.36, UV, $/m3
# 5.37, UV, $/Unit
# 5.38, UV, $/head
# 5.39, UV, $/1000 head


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
  years    = as.character(years)
)


computeData <- function(reporter = NA) {

  if (use_previous && file.exists(paste0(DB_rds_storage, reporter, '.rds'))) {
    NULL
  } else {

    morder <- 3
    rmNA <- FALSE

    data <- getComputedDataSWS(reporter)

    # XXX 10 should be the mimimum, but it's not necessarily true.
    # (a zero-row df will be returned for non-existing countries, in
    # any case (e.g., USSR before 1991))
    if (nrow(data) > 10 & any(c(5608, 5609, 5908, 5909) %in% unique(data$measuredElementTrade))) {
      data <- data %>%
        reshapeTrade() %>%
        # XXX should be "value only" items
        dplyr::filter(!is.na(qty)) %>%
        dplyr::mutate(uv = value / qty) %>%
        dplyr::select(
          flow,
          geographicAreaM49Reporter,
          geographicAreaM49Partner,
          measuredItemCPC,
          timePointYears,
          qty,
          value,
          weight,
          uv,
          flag_qty,
          flag_value,
          flag_weight
        ) %>%
        tidyr::complete(
          tidyr::nesting(
            flow,
            geographicAreaM49Reporter,
            geographicAreaM49Partner,
            measuredItemCPC
          ),
          timePointYears
        ) %>%
        dplyr::group_by(
          flow,
          geographicAreaM49Reporter,
          geographicAreaM49Partner,
          measuredItemCPC
        ) %>%
        dplyr::mutate(
          ma_v_bck_nan = RcppRoll::roll_mean(lag(value),
                           morder, fill = NA, align = 'right', na.rm = rmNA),
          ma_q_bck_nan = RcppRoll::roll_mean(lag(qty),
                           morder, fill = NA, align = 'right', na.rm = rmNA)
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
          ma_v_fwd_nan = RcppRoll::roll_mean(lead(value),
                           morder, fill = NA, align = 'right', na.rm = rmNA),
          ma_q_fwd_nan = RcppRoll::roll_mean(lead(qty),
                           morder, fill = NA, align = 'right', na.rm = rmNA)
        ) %>%
        dplyr::arrange(
          flow,
          geographicAreaM49Reporter,
          geographicAreaM49Partner,
          measuredItemCPC,
          timePointYears
        ) %>%
        dplyr::mutate(
          x         = 1:n(),
          maqn      = ifelse(x <= morder, ma_q_fwd_nan, ma_q_bck_nan),
          mavn      = ifelse(x <= morder, ma_v_fwd_nan, ma_v_bck_nan),
          man       = mavn/maqn,
          ratio_man = uv/man,
          outn      = if_else(between(ratio_man, 1-threshold, 1+threshold),
                        0, 1, 0)
        ) %>%
      dplyr::ungroup()

      saveRDS(data, paste0(DB_rds_storage, reporter, '.rds'))

      rm(data)

      invisible(gc())

    } else {
      NULL
    }


    #print('OK')

    #data
  }

}

boxB1 <- function (x, method = "asymmetric", k = 1.5,
                   id = NULL, exclude = NA, logt = FALSE) {
    if (is.null(id)) id <- 1:length(x)

    tst <- x %in% exclude

    if (sum(tst) == 0) {
      to.check <- NULL
      yy <- x
      lab <- id
    } else {
      to.check <- id[tst]
      yy <- x[!tst]
      lab <- id[!tst]
    }

    #message("The following values are excluded from computations: ", exclude)

    #message("The excluded obs. are: ", sum(tst))

    if (logt) {
      yy <- log(yy + 1)
      #message("Please note that results refer to log+1 transformed data")
    }

    qq <- quantile(x = yy, probs = c(0.25, 0.5, 0.75), na.rm=TRUE)

    #message("data: ", paste(x, collapse = '#'))
    #message("1st Quartile: ", round(qq[1], 5))
    #message("Median: ", round(qq[2], 5))
    #message("3rd Quartile: ", round(qq[3], 5))

    if (method == "resistant" | method == "boxplot") {
      vql <- vqu <- qq[3] - qq[1]
      low.b <- qq[1] - k * vql
      up.b <- qq[3] + k * vqu
    }

    if (method == "asymmetric") {
      vql <- qq[2] - qq[1]
      vqu <- qq[3] - qq[2]
      low.b <- qq[1] - 2 * k * vql
      up.b <- qq[3] + 2 * k * vqu
    }

    if (method == "adjbox") {
      warning("With MedCouple the argument k is set equal to 1.5")
      medc <- robustbase::mc(yy)
      #message("The MedCouple skewness measure is: ", round(medc, 5))
      aa <- robustbase::adjboxStats(x = yy)
      low.b <- aa$fence[1]
      up.b <- aa$fence[2]
    }

    names(low.b) <- "low"
    names(up.b) <- "up"
    outl <- ((yy < low.b) | (yy > up.b)) %in% TRUE
    #message("The lower bound is: ", round(low.b, 5))
    #message("The upper bound is: ", round(up.b, 5))
    #message("No. of outliers in left tail: ", sum(yy < low.b))
    #message("No. of outliers in right tail: ", sum(yy > up.b), "\n")
    if (sum(outl) == 0) {
      #message("No outliers found!")
      fine <- list(fences = c(lower = low.b, upper = up.b),
                   excluded = to.check)
    } else {
      fine <- list(fences = c(lower = low.b, upper = up.b), 
                   excluded = to.check, outliers = lab[outl])
    }

    return(fine)
}



myboxB <- function(x, method = NA, k = 1.5, logt = FALSE) {
	res <- try(boxB1(x, method = method, k = k, logt = logt)$outliers)
	if (class(res) == 'try-error' | is.null(res)) {
		return(rep(0, length(x)))
	} else {
		return((1:length(x) %in% res)*1)
	}
}


myfun_build_db_for_app <- function(rep = NA) {

  readRDS(paste0(DB_rds_storage, rep, '.rds')) %>%
    # XXX files ar saved with ALL NAs generated by completing
    # the dataset: Should be removed there as they are in any
    # case removed here.
    dplyr::filter(!is.na(value)) %>%
    dplyr::select(
      flow:uv,
      man,
      maqn,
      mavn,
      flag_qty,
      flag_value,
      flag_weight
    ) %>%
    dplyr::rename(
      unit_value       = uv,
      movav_qty        = maqn,
      movav_value      = mavn,
      movav_unit_value = man
  )
}

##' # Set up parallel processing requirements
##'
##' If `multicore` = `TRUE` some preparatory stepd need to be performed:
##' Initialise the required packages and export objects to the cores.

if (multicore) {
  # XXX is this actually required?
  #if (CheckDebug()) {
    n_cores <- parallel::detectCores() - 1
  #} else {
  #  n_cores <- 3
  #}

  cl <- parallel::makeCluster(n_cores)
  doParallel::registerDoParallel(cl)

  parallel::clusterEvalQ(cl, {
                         library(faosws)
                         library(dplyr)
                         NULL
               })

  parallel::clusterExport(cl,
                          c('getComputedDataSWS',
                            'years',
                            'Vars',
                            'Keys',
                            'dir_to_save',
                            'DB_rds_storage',
                            'reshapeTrade',
                            'threshold',
                            'computeData',
                            'myfun_build_db_for_app',
                            'boxB1',
                            'myboxB',
                            'use_previous',
                            'print_log')
                          )

  parallel::clusterExport(cl, c(ls(pattern = 'swsContext')))

  if (CheckDebug()) {

    parallel::clusterExport(cl, 'SETTINGS')

    parallel::clusterEvalQ(cl,
      {
        ## Define where your certificates are stored
        faosws::SetClientFiles(SETTINGS[["certdir"]])
        NULL
      }
    )
  }
}

##' # Download data
##'
##' A query to the SWS with all reporters, partners, items, elemtents
##' is performed, by reporter. The result of each reporter-specific query
##' gets completed for all years and for all existing partner/flow/item
##' combinations. The expansion for all years is necessary as calculations
##' that need a complete time series (even with NA values) is required.
##'
##' Once a reporter-specific dataset is downloaded and completed, moving
##' averages of unit value and the ratios of the unit value with respect
##' to these averages are calculated. A first version of "outlier" ($outn$)
##' that uses information only at the reporter-level is calculated as:
##'
##' $$
##' outn =
##'   \begin{cases}
##'     \text{TRUE},  & \text{if $ratio < 1 - threshold$ or $ratio > 1 + threshold$}\\
##'     \text{FALSE}, & \text{otherwise}
##'   \end{cases}
##' $$
##'
##' where $threshold$ is the `threshold` parameter (as of 2018-03-09 it
##' is set to 0.5), $ratio = uv / \overline{uv}$ ($\overline{uv}$ is the
##' `morder` (3 as of 2018-03-09) moving average of $uv$).
##'
##' The reporter-specific datasets get stored in the "shared drive" of
##' the server for latter use.

print_log('Start computeData loop')
start_time <- Sys.time()
plyr::m_ply(
  #expand.grid(
  #  reporters,
  #  stringsAsFactors = FALSE
  #  ) %>%
  #    as_data_frame() %>%
  #    dplyr::rename(reporter = Var1, year = Var2),
  reporters,
  .fun      = computeData,
  .parallel = multicore,
  .progress = 'text'
)
end_time <- Sys.time()
print(end_time - start_time)
print_log('End computeData loop')

invisible(gc())


##' Data assembly and further operations
##'
##' Once all reporter-specific datasets are available, they are assembled
##' together in order to compute statistics that uses all reporters'
##' information, specifically, the following outliers are identified:
##'
##' - `outmw100`: equal to `TRUE` if the unit value is less or greater
##' than the median unit value of the item for all reporters;
##' - `outM`: a boxplot approach is used to define this outlier. In
##' particular, if the unit value falls outside the boxplot whiskers
##' it is considered an outlier. To overcome some asymmetry in the
##' the distribution of unit values, they are transformed into logarithmns.
##' - `outp`: defined as $outn$, but the threshold is not fixed for all
##' items, but is item-specific and the lower and upper thresholds correspond
##' to the 5th and 95th percentile of the distribution of the unit value
##' for the item for all reporters and flows.
##'
##' By default, the "outlier" variable is defined to be equal to $outn$,
##' though in the validation tool it is possible to choose which method
##' to use when defining what an outlier is.


print_log('Start sub RDS loop')
start_time <- Sys.time()

db_save <- plyr::mdply(
    sub('.rds', '', dir(DB_rds_storage)),
    .fun      = myfun_build_db_for_app,
    .parallel = multicore,
    .progress = 'text'
  ) %>%
  tbl_df() %>%
  select(-X1) %>%
  dplyr::mutate(ratio_man = unit_value / movav_unit_value) %>%
  group_by(flow, measuredItemCPC, timePointYears) %>%
  dplyr::mutate(
    # median_world
    avg_world    = sum(value, na.rm = TRUE)/sum(qty, na.rm = TRUE),
    median_world = median(unit_value, na.rm=TRUE),
    outM2        = myboxB(unit_value, method = 'asymmetric', k = 1.5, logt = TRUE),
    outM2        = as.integer(outM2)
  ) %>%
  group_by(geographicAreaM49Reporter, flow, measuredItemCPC, timePointYears) %>%
  dplyr::mutate(
    median = median(unit_value, na.rm=TRUE),
    avg    = sum(value, na.rm = TRUE) / sum(qty, na.rm = TRUE),
    n      = n()
  ) %>%
  group_by(measuredItemCPC) %>%
  dplyr::mutate(
    p05n = quantile(ratio_man, 0.05, na.rm=TRUE),
    p95n = quantile(ratio_man, 0.95, na.rm=TRUE)
  ) %>%
  ungroup() %>%
  dplyr::mutate(
    # outman = outn
    outman   = if_else(between(ratio_man, 1-threshold, 1+threshold), 0L, 1L, 0L),
    outmw100 = if_else(unit_value < 0.01*median_world | unit_value > 100*median_world, 1L, 0L, 0L),
    outM     = if_else(outM2 == 1, 1L, 0L, 0L),
    outp     = if_else(ratio_man < p05n | ratio_man > p95n, 1L, 0L, 0L),
    # Default variables
    out      = outman,
    ma       = movav_unit_value
  )

end_time <- Sys.time()
print(end_time - start_time)
print_log('End sub RDS loop')

if (multicore) {
  parallel::stopCluster(cl)
}

##' # Save data
##'
##' Data is ready to be saved. Before doing so, some informational variables
##' are added:
##'
##' - `perc.value` and `perc.qty`: give the importance (percentage) of values
##' and quantities of a specific item in the reporters' total trade.
##' - reporter, partner, and commodity names: SWS does not return the names
##' of the dimensions, so this information is retrieved and joined to the
##' dataset.
##'
##' After the additional information is added, the dataset is saved to the
##' "shared drive" from where the validation tool will read it.

db_save <- db_save %>%
  select(
         flow,
         geographicAreaM49Reporter,
         geographicAreaM49Partner,
         measuredItemCPC,
         timePointYears,
         qty,
         value,
         weight,
         flag_qty,
         flag_value,
         flag_weight,
         unit_value,
         out,
         ma,
         outman,
         outmw100,
         outp,
         outM2,
         outM
  ) %>%
  # Team BC suggested to remove mirrorred data
  # (flag_value is more general)
  dplyr::filter(!grepl('^T', flag_value)) %>%
  # Adding percentages of values and quantities.
  group_by(flow, geographicAreaM49Reporter, timePointYears) %>%
  dplyr::mutate(tot.value = sum(value, na.rm = TRUE), perc.value = value / tot.value) %>%
  group_by(flow, geographicAreaM49Reporter, measuredItemCPC, timePointYears) %>%
  dplyr::mutate(tot.qty = sum(qty, na.rm = TRUE), perc.qty = qty / tot.qty) %>%
  ungroup() %>%
  select(-tot.value, -tot.qty)

geographicAreaM49Reporter_names <- db_save %>%
  select(geographicAreaM49Reporter) %>%
  distinct() %>%
  data.table::as.data.table() %>%
  faoswsUtil::nameData('trade', 'completed_tf_cpc_m49', .) %>%
  dplyr::rename(reporter_name = geographicAreaM49Reporter_description)

geographicAreaM49Partner_names <- db_save %>%
  select(geographicAreaM49Partner) %>%
  distinct() %>%
  data.table::as.data.table() %>%
  faoswsUtil::nameData('trade', 'completed_tf_cpc_m49', .) %>%
  dplyr::rename(partner_name = geographicAreaM49Partner_description)

measuredItemCPC_names <- db_save %>%
  select(measuredItemCPC) %>%
  distinct() %>%
  data.table::as.data.table() %>%
  faoswsUtil::nameData('trade', 'completed_tf_cpc_m49', .) %>%
  dplyr::rename(item_name = measuredItemCPC_description) %>%
  # https://github.com/SWS-Methodology/tradeValidationTool/issues/9
  dplyr::mutate(item_name = gsub('\\b([Mm]at).\\b', '\\1e', item_name))

db_save <- left_join(
  db_save,
  geographicAreaM49Reporter_names,
  by = 'geographicAreaM49Reporter'
)

db_save <- left_join(
  db_save,
  geographicAreaM49Partner_names,
  by = 'geographicAreaM49Partner'
)

db_save <- left_join(
  db_save,
  measuredItemCPC_names,
  by = 'measuredItemCPC'
)

print_log(
  paste('First year is', min(db_save$timePointYears),
        ', last year is', max(db_save$timePointYears))
)

#str(db_save)
#
#head(db_save)

saveRDS(db_save, paste0(dir_to_save, name_to_save))

end_message <- paste0('The dataset for validation should have been saved in ',
            dir_to_save, name_to_save)

if (!CheckDebug()) {
  send_mail(
    from    = "SWS-trade-module@fao.org",
    to      = swsContext.userEmail,
    subject = "Validation trade plugin ran successfully",
    body    = end_message
  )
}

end_message

