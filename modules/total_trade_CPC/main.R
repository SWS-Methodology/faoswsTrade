##' ---
##' title: "Appendix: `total_trade_CPC` module"
##' author:
##'   - Marco Garieri
##'   - Alexander Matrunich
##'   - Christian A. Mongeau Ospina
##'   - Aydan Selek
##'   - Bo Werth\
##'
##'     Food and Agriculture Organization of the United Nations
##' date: "`r format(Sys.time(), '%e %B %Y')`"
##' output:
##'    pdf_document
##' ---

##' This module aggregates total trade flow by reporting country for partners
##' countries to a single total trade for each unique CPC commodity code. The
##' module saves the output into the dataset `total\_trade\_cpc\_m49`,
##' within the `trade` domain.

message("TOT: version 20200203")

##+ setup, include=FALSE
knitr::opts_chunk$set(echo = FALSE, eval = FALSE)

library(data.table)
library(faoswsTrade)
library(faosws)
library(stringr)
library(scales)
library(faoswsUtil)
library(faoswsFlag)
library(tidyr)
library(dplyr, warn.conflicts = FALSE)

##+ init

# If this is set to TRUE, the module will download the whole dataset
# saved on SWS (year specific) and will do a setdiff by comparing this
# set and the dataset generated by the module: all values saved on SWS
# that are not generated by the current run should be considered "wrong"
# (e.g., generated by a previous run of the module that had a bug) and
# will then be set to NA. See issue #164
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
  SETTINGS = ReadSettings("modules/total_trade_CPC/sws.yml")
  ## Define where your certificates are stored
  faosws::SetClientFiles(SETTINGS[["certdir"]])
  ## Get session information from SWS. Token must be obtained from web interface
  GetTestEnvironment(baseUrl = SETTINGS[["server"]],
                     token = SETTINGS[["token"]])
}

# E-mail addresses of people that will get notified.
EMAIL_RECIPIENTS <- ReadDatatable("ess_trade_people")$fao_email
EMAIL_RECIPIENTS <- gsub(" ", "", EMAIL_RECIPIENTS)

# Remove S.T.:
EMAIL_RECIPIENTS <- EMAIL_RECIPIENTS[!grepl("yy", EMAIL_RECIPIENTS)]

##' # Parameters

##' - `year`: year for processing.
year <- as.integer(swsContext.computationParams$year)

startTime = Sys.time()

#if (!CheckDebug()) {
#  updateInfoTable(
#    year  = year,
#    table = 'total_tf_runs_info',
#    mode  = 'restart'
#  )
#}

##+ import

##' # Import Data from Complete TF CPC
##'
##' Import monetary values and quantities from data previously
##' generated by the `complete_tf_cpc` module.
##'
##' The query is done for all reporters, all partners, all elements
##' (indicated below), all items, and the year set in `year`.
##'
##' 1. `5607`: Import Quantity (number)
##' 1. `5608`: Import Quantity (heads)
##' 1. `5609`: Import Quantity (1000 heads)
##' 1. `5610`: Import Quantity (tonnes)
##' 1. `5907`: Export Quantity (number)
##' 1. `5908`: Export Quantity (heads)
##' 1. `5909`: Export Quantity (1000 heads)
##' 1. `5910`: Export Quantity (tonnes)
##' 1. `5622`: Imports (1,000 US$)
##' 1. `5922`: Exports (1,000 US$)

allReportersDim <-
  GetCodeList("trade", "completed_tf_cpc_m49", "geographicAreaM49Reporter")[type == "country", code] %>%
  Dimension(name = "geographicAreaM49Reporter", keys = .)

allPartnersDim <-
  GetCodeList("trade", "completed_tf_cpc_m49", "geographicAreaM49Partner")[type == "country", code] %>%
  Dimension(name = "geographicAreaM49Partner", keys = .)

allElementsDim <-
  c("5607", "5608", "5609", "5610", "5907", "5908", "5909", "5910", "5622", "5922") %>%
  Dimension(name = "measuredElementTrade", keys = .)

allItemsDim <-
  GetCodeList("trade", "completed_tf_cpc_m49", "measuredItemCPC")[,code] %>%
  Dimension(name = "measuredItemCPC", keys = .)

allYearsDim <- Dimension(name = "timePointYears", keys = as.character(year))

completetradekey <-
  DatasetKey(
    domain = "trade",
    dataset = "completed_tf_cpc_m49",
      dimensions =
        list(
          allReportersDim,
          allPartnersDim,
          allElementsDim,
          allItemsDim,
          allYearsDim
        )
  )

message("TOT: get data")

completetrade <- GetData(completetradekey)

stopifnot(nrow(completetrade) > 0)

setnames(completetrade, "geographicAreaM49Reporter", "geographicAreaM49")

##' # Aggregate values across partner dimension
##'
##' Bilateral trade data (from the `complete_tf_cpc` module) for each
##' reporter/flow/year/item combination is aggregated for all partners.
##'
##' Note that:
##'
##' - missing values are ignored in the aggregation (i.e., set to zero)
##' - the aggregation of flags is done using the `flagWeightTable` from
##'   the `faoswsFlag` package.

##+ flagWeightTable, results='asis'
knitr::kable(faoswsFlag::flagWeightTable)

##+ aggregate

flagWeightTable_status <- frame_data(
  ~flagObservationStatus, ~flagObservationWeights,
  'X',                   1.00,
  '',                    0.99,
  'T',                   0.80,
  'E',                   0.75,
  'I',                   0.50,
  'M',                   0.00
)

# This shouldn't ever be needed as all values are a sum ("s")
# XXX No, not really: there are some reporters that for some
# commodities and flow have just one partner
flagWeightTable_method <- frame_data(
  ~flagObservationStatus, ~flagObservationWeights,
  'h',                   1.00,
  # XXX check why some are blanks
  '',                    0.99,
  'q',                   0.95,
  'p',                   0.90,
  'i',                   0.80,
  'e',                   0.60,
  'f',                   0.50,
  'c',                   0.40,
  '-',                   0.30,
  's',                   0.20
)

message("TOT: flagWeightTable_status_no_imp")

# This table is going to be used in cases where imputations account
# for less than 10% of the total flow: X is a flag that never shows
# up here (was used in complete trade as "official", replaced by
# blank), and we replace it with "I" as all flags need to be present.
# After the substitution, "I" will get the maximum value, so for
# practical purposes, will never be set (as there are other flows
# with a flag different from "I" with a lower value)
flagWeightTable_status_no_imp <-
  flagWeightTable_status %>%
  filter(flagObservationStatus != "I") %>%
  mutate(flagObservationStatus = ifelse(flagObservationStatus == "X", "I", flagObservationStatus))

message("TOT: diff_flags")

completetrade[,
  `:=`(
    nobs = .N,
    diff_flags = length(unique(flagObservationStatus)) > 1,
    perc_imputed = sum(Value[flagObservationStatus == "I"]) / sum(Value)
  ),
  .(geographicAreaM49, timePointYears, measuredItemCPC, measuredElementTrade)
]

MIN_IMPUTED <- 0.1

message("TOT: rbind")

total_trade_cpc_wo_uv <-
  rbind(
    # One obs
    completetrade[
      nobs == 1,
      .(geographicAreaM49, timePointYears, measuredItemCPC, measuredElementTrade, Value, flagObservationStatus, flagMethod)],
    # Multiple obs, one flag
    completetrade[
      nobs > 1 & diff_flags == FALSE,
      .(Value = sum(Value, na.rm = TRUE), flagObservationStatus = unique(flagObservationStatus), flagMethod = "s"),
      .(geographicAreaM49, timePointYears, measuredItemCPC, measuredElementTrade)
    ],
    # Multiple obs, multiple flags, imputed > 10%
    completetrade[
      nobs > 1 & diff_flags == TRUE & perc_imputed > MIN_IMPUTED,
      .(
        Value = sum(Value, na.rm = TRUE),
        flagObservationStatus =
          aggregateObservationFlag(
            flagObservationStatus,
            flagTable = flagWeightTable_status
          ),
        # In any case, 's' is the weakest flag, so that if aggregation
        # was performed, then 's' is the final Method flag.
        flagMethod = "s"
      ),
      .(geographicAreaM49, timePointYears, measuredItemCPC, measuredElementTrade)
    ],
    # Multiple obs, multiple flags, imputed <= 10%
    completetrade[
      nobs > 1 & diff_flags == TRUE & perc_imputed <= MIN_IMPUTED,
      .(
        Value = sum(Value, na.rm = TRUE),
        flagObservationStatus =
          aggregateObservationFlag(
            flagObservationStatus,
            flagTable = flagWeightTable_status_no_imp
          ),
        # In any case, 's' is the weakest flag, so that if aggregation
        # was performed, then 's' is the final Method flag.
        flagMethod = "s"
      ),
      .(geographicAreaM49, timePointYears, measuredItemCPC, measuredElementTrade)
    ]
  )

completetrade[, `:=`(nobs = NULL, diff_flags = NULL, perc_imputed = NULL)]

message("TOT: n > 4")

# Data for which weight and numbers were computed
# (n == 4 => (value, qty) * (import, export))
qty_and_weight <-
  completetrade[,
    .(n = length(unique(measuredElementTrade))),
    measuredItemCPC
  ][
    n > 4
  ][,
    `:=`(out = TRUE, n = NULL)
  ]

qty_and_weight <-
  rbind(
    data.table(qty_and_weight, measuredElementTrade = '5610'),
    data.table(qty_and_weight, measuredElementTrade = '5910')
  )

message("TOT: keep weight livestock")

# Keep only weights of livestock
total_trade_cpc_weight_livestock <-
  qty_and_weight[
    total_trade_cpc_wo_uv,
    on = c("measuredItemCPC", "measuredElementTrade")
  ][
    out == TRUE
  ][,
    out := NULL
  ]

message("TOT: rm weight livestock")

# Remove weights of livestok (keeping heads)
total_trade_cpc_weight_livestock <-
  qty_and_weight[
    total_trade_cpc_wo_uv,
    on = c("measuredItemCPC", "measuredElementTrade")
  ][
    is.na(out)
  ][,
    out := NULL
  ]

##' # Calculate Unit Values
##'
##' Calculate unit value (US$ per quantity unit) at CPC level if the
##' quantity is greater than zero (if the quantity happens to be equal
##' to zero, the unit value is set to NA).
##'
##' - use `flagObservationsStatus` from quantity measures
##' - set `flagMethod` to `i` for unit values (calculated as identity)
##'
##' The created elements are:
##'
##' - `5630`: Import Unit Value (US$ / tonne)
##' - `5638`: Import Unit Value (US$ / heads)
##' - `5639`: Import Unit Value (US$ / 1000 heads)
##' - `5930`: Export Unit Value (US$ / tonne)
##' - `5938`: Export Unit Value (US$ / heads)
##' - `5939`: Export Unit Value (US$ / 1000 heads)

##+ unit-value

addUV <- function(data) {

  var_names <- c("Value", GetDatasetConfig("trade", "total_trade_cpc_m49")$dimensions, GetDatasetConfig("trade", "total_trade_cpc_m49")$flags)

  ## data <- total_trade_cpc
  copyData <- data %>%
    select_(.dots = var_names)


  copyData$unit <- ifelse(copyData$measuredElementTrade %in% c("5622", "5922"), "monetary", "quantity")
  copyData$flow <- ifelse(substr(copyData$measuredElementTrade, 1, 2) == "56", "import", "export")

  copyData_quantity <-
    copyData %>%
    dplyr::filter(unit == "quantity") %>%
    dplyr::select(
      -unit,
      measuredElementTrade.qty = measuredElementTrade,
      -flagMethod
    )
    ## select(-unit) # must keep to assign proper elemnt code to unit value

  copyData_monetary <-
    copyData %>%
    dplyr::filter(unit == "monetary") %>%
    dplyr::select(
      -unit,
      -measuredElementTrade,
      -flagObservationStatus,
      -flagMethod
    )

  me_qty_uv <-
    data.frame(
      flow = c(rep("import", 4), rep("export", 4)),
      measuredElementTrade.qty = c("5607", "5608", "5609", "5610", "5907", "5908", "5909", "5910"),
      measuredElementTrade = c("5637", "5638", "5639", "5630", "5937", "5938", "5939", "5930"),
      stringsAsFactors = FALSE
    )

  copyData_uv <-
    copyData_quantity %>%
    left_join(
      copyData_monetary,
      by = c("geographicAreaM49", "timePointYears", "measuredItemCPC","flow"),
      suffix = c(".qty", ".mon")
    ) %>%
    dplyr::mutate(
      Value = ifelse(Value.qty > 0, Value.mon * 1000 / Value.qty, NA)
    ) %>%
    left_join(me_qty_uv, by = c("measuredElementTrade.qty", "flow")) %>%
    ## ## only keep columns already present in input data set
    dplyr::mutate(flagMethod = "i") %>%
    select_(.dots = var_names)

  return(copyData_uv)

}

message("TOT: total_trade_cpc_all_no_uv")

total_trade_cpc_all_no_uv <-
  rbind(
    data.table(total_trade_cpc_wo_uv, livestock_w = FALSE),
    data.table(total_trade_cpc_weight_livestock, livestock_w = TRUE)
  )

table(total_trade_cpc_all_no_uv$flagObservationStatus, total_trade_cpc_all_no_uv$flagMethod)

##' # Remove "non-existent" transactions
##'
##' It can happen that a given observation was generated by a previous
##' run of the module, but in a subsequent run it is not generated. This
##' happens mainly because bug-fixes were introduced so that an observation
##' that should not have existed is, correctly, not generated by a fixed
##' version of the module. If no overwrite of the observation that should
##' not exist happens, then it lives in the dataset "forever". Thus, the
##' latest version of the data is downloaded, it is checked which observations
##' are not generated by the current run of the module (considered the most
##' correct version), and all observations that were previously saved in the
##' dataset but are not in the current output are set to NA. This increases
##' the computation time of the module (it needs to download the complete
##' trade dataset), but guarantees that data that should not exist is not
##' saved in the dataset.

##' # Remove "protected" data from the module's output.
##'
##' Combinations of dimensions that correspond to "protected" data are
##' removed from the module output as these should not overwrite the
##' data already in SWS.

message("TOT: nonexistent")

if (remove_nonexistent_transactions == TRUE) {
  #flog.trace("[%s] Remove non-existent transactions (RNET)", PID, name = "dev")

  allReportersDim_tot <-
    GetCodeList("trade", "completed_tf_cpc_m49", "geographicAreaM49Reporter")[type == "country", code] %>%
    # Using the bilateral one when "type" was removed
    #GetCodeList("trade", "total_trade_cpc_m49", "geographicAreaM49")[type == "country", code] %>%
    Dimension(name = "geographicAreaM49", keys = .)

  allElementsDim_tot <-
    c("5607", "5608", "5609", "5610", "5907", "5908", "5909", "5910", "5622", "5922", #) %>% #,
      ## UV elements:
      "5637", "5638", "5639", "5630", "5937", "5938", "5939", "5930") %>%
    Dimension(name = "measuredElementTrade", keys = .)

  allItemsDim_tot <-
    GetCodeList("trade", "total_trade_cpc_m49", "measuredItemCPC")[,code] %>%
    Dimension(name = "measuredItemCPC", keys = .)

  totaltradekey <-
    DatasetKey(
      domain = "trade",
      dataset = "total_trade_cpc_m49",
        dimensions =
          list(
            allReportersDim_tot,
            allElementsDim_tot,
            allItemsDim_tot,
            allYearsDim
          )
    )

  #flog.trace("[%s] RNET: Download existent SWS dataset", PID, name = "dev")

  message("TOT: existing_data")

  existing_data <- GetData(key = totaltradekey, omitna = TRUE)

  #flog.trace("[%s] Keep protected data", PID, name = "dev")

  message("TOT: protected_data")

  # some flags are "protected", i.e., data with these flags
  # should not be overwritten/removed
  protected_data <-
    flagValidTable[
      existing_data,
      on = c('flagObservationStatus', 'flagMethod')
    ][
      # Unprotect these
      (flagObservationStatus == 'T' &  flagMethod == 'c') |
      (flagObservationStatus == 'I' &  flagMethod == 'c') |
      (flagObservationStatus == ''  &  flagMethod == 'c') |
      (flagObservationStatus == ''  &  flagMethod == 'h'),
      Protected := FALSE
    ][
      # Protect these
      (flagObservationStatus == 'T'   &  flagMethod == 'q') |
      (flagObservationStatus == 'X'   &  flagMethod == 'p'),
      Protected := TRUE
    ][
      Protected %in% TRUE
    ]

  message("TOT: new_protected_data")

  # This data is not generated by the module, but need to be kept
  # (e.g., because it's new data from external sources)
  new_protected_data <-
    copy(protected_data) %>%
    #dplyr::select(-Valid, -Protected) %>%
    anti_join(
      total_trade_cpc_all_no_uv,
      by = c('geographicAreaM49', 'timePointYears', 'measuredItemCPC', 'measuredElementTrade')
    )

  message("TOT: set flags protected_data")

  protected_data[,
      `:=`(
        flagObservationStatus_p = flagObservationStatus,
        flagMethod_p = flagMethod,
        Value_p = Value,
        flagObservationStatus = NULL,
        flagMethod = NULL,
        Value = NULL
      )
    ]

  message("TOT: total_trade_cpc_all_no_uv")

  total_trade_cpc_all_no_uv <-
    protected_data[
      total_trade_cpc_all_no_uv,
      on = c('timePointYears', 'geographicAreaM49', 'measuredElementTrade', 'measuredItemCPC')
    ][
      !is.na(Value_p),
      `:=`(Value = Value_p, flagObservationStatus = flagObservationStatus_p, flagMethod = flagMethod_p)
    ][,
      `:=`(Value_p = NULL, flagObservationStatus_p = NULL, flagMethod_p = NULL)
    ]

  if (nrow(new_protected_data) > 0) {
    total_trade_cpc_all_no_uv <-
      rbind(total_trade_cpc_all_no_uv, new_protected_data, fill = TRUE)
  }

  total_trade_cpc_uv <-
    # Only for what we need (livestock *weight* is optionally reported)
    addUV(total_trade_cpc_all_no_uv[!(livestock_w %in% TRUE)]) %>%
    dplyr::filter(!is.na(Value))

  total_trade_cpc_w_uv <-
    rbind(total_trade_cpc_all_no_uv, total_trade_cpc_uv, fill = TRUE) %>%
    setDT()

  # Remove protected data
  total_trade_cpc_w_uv <-
    total_trade_cpc_w_uv[
      !(Protected %in% TRUE)
    ][,
      `:=`(Protected = NULL, Valid = NULL, livestock_w = NULL)
    ]

  message("TOT: data_diff")

  # Difference between what was saved and what the module produced:
  # whatever is not produced in the run should be set to NA. See #164
  # (No need of year as key as all data refer to the same year)
  data_diff <-
    existing_data[!total_trade_cpc_w_uv,
                  on = c('geographicAreaM49',
                         'measuredElementTrade',
                         'measuredItemCPC')]

  data_diff <-
    flagValidTable[
      data_diff,
      on = c('flagObservationStatus', 'flagMethod')
    ][
      # Unprotect these
      (flagObservationStatus == 'T' &  flagMethod == 'c') |
      (flagObservationStatus == 'I' &  flagMethod == 'c') |
      (flagObservationStatus == ''  &  flagMethod == 'c') |
      (flagObservationStatus == ''  &  flagMethod == 'h'),
      Protected := FALSE
    ][
      # Protect these
      (flagObservationStatus == 'T'   &  flagMethod == 'q') |
      (flagObservationStatus == 'X'   &  flagMethod == 'p'),
      Protected := TRUE
    ][
      Protected %in% FALSE
    ][,
      `:=`(Protected = NULL, Valid = NULL)
    ]

  if (nrow(data_diff) > 0) {
    #flog.trace("[%s] RNET: Non-existent transactions set to NA", PID, name = "dev")

    data_diff[,`:=`(Value                 = NA_real_,
                    flagObservationStatus = NA_character_,
                    flagMethod            = NA_character_)]

    total_trade_cpc_w_uv <- rbind(total_trade_cpc_w_uv, data_diff)
  } #else {
    #flog.trace("[%s] RNET: There are no non-existent transactions", PID, name = "dev")
  #}
} else {
  ######################################################
  #  FIXME: this case does NOT keep protected flows    #
  #  (usually, remove_nonexistent_transactions = TRUE) #
  ######################################################
  total_trade_cpc_uv <-
    addUV(total_trade_cpc_all_no_uv %>% dplyr::filter(livestock_w == FALSE)) %>%
    dplyr::filter(!is.na(Value))

  total_trade_cpc_w_uv <-
    bind_rows(
      total_trade_cpc_uv,
      total_trade_cpc_all_no_uv
    ) %>%
    setDT()

  total_trade_cpc_w_uv[, livestock_w := NULL]
}

setcolorder(total_trade_cpc_w_uv,
            c("geographicAreaM49", "measuredElementTrade", "measuredItemCPC",
              "timePointYears", "Value", "flagObservationStatus", "flagMethod"))

message("TOT: rm UV t and h")

# Remove unit values in t for items that have both t and h
total_trade_cpc_w_uv <-
  total_trade_cpc_w_uv[!(substr(measuredElementTrade, 3, 4) == "30" &
                         measuredItemCPC %chin% qty_and_weight$measuredItemCPC)]


######### Remove unchanged flows

total_trade_cpc_w_uv[, timePointYears := as.character(timePointYears)]

setnames(
  existing_data,
  c("Value", "flagObservationStatus", "flagMethod"),
  c("Value_ex", "flagObservationStatus_ex", "flagMethod_ex")
)

total_trade_cpc_w_uv_changed <-
  existing_data[
    total_trade_cpc_w_uv,
    on = c("geographicAreaM49", "measuredElementTrade",
		   "measuredItemCPC", "timePointYears")
  ][
    !((dplyr::near(Value, Value_ex, tol = 0.000001) &
       flagObservationStatus == flagObservationStatus_ex &
       flagMethod == flagMethod_ex) %in% TRUE)
  ]

total_trade_cpc_w_uv_changed[, c("Value_ex", "flagObservationStatus_ex", "flagMethod_ex") := NULL]


##' # Save data
##'
##' Saved data will be available in the "Total Trade (CPC)" dataset
##' (`total_trade_cpc_m49`) of the `trade` domain.

message("TOT: saving")

stats <- SaveData("trade",
                  "total_trade_cpc_m49",
                  total_trade_cpc_w_uv_changed, waitTimeout = Inf)

end_message <- sprintf(
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

# Compute and save "Agricult.Products, Total" (CPC: F1882)

aggregate_groups_tp <- ReadDatatable("aggregate_groups", where = "domain_code = 'TP' AND var_type = 'item'")

stopifnot(nrow(aggregate_groups_tp) > 0)

aggregate_groups_tp_items <- unique(aggregate_groups_tp[!is.na(var_code_sws)]$var_code_sws)

aggregate_groups_tp_items <- setdiff(aggregate_groups_tp_items, c("F1159", "F1267"))

allReportersDim_tot <-
  GetCodeList("trade", "completed_tf_cpc_m49", "geographicAreaM49Reporter")[type == "country", code] %>%
  # Using the bilateral one when "type" was removed
  #GetCodeList("trade", "total_trade_cpc_m49", "geographicAreaM49")[type == "country", code] %>%
  Dimension(name = "geographicAreaM49", keys = .)

allElementsDim_tot <- Dimension(name = "measuredElementTrade", keys = c("5622", "5922"))

allItemsDim_tot <-
  aggregate_groups_tp_items %>%
  Dimension(name = "measuredItemCPC", keys = .)

agrickey <-
  DatasetKey(
    domain = "trade",
    dataset = "total_trade_cpc_m49",
      dimensions =
        list(
          allReportersDim_tot,
          allElementsDim_tot,
          allItemsDim_tot,
          allYearsDim
        )
  )

#flog.trace("[%s] RNET: Download existent SWS dataset", PID, name = "dev")

message("TOT: agric_data")

agric_data <- GetData(key = agrickey, omitna = TRUE)

agric_data <- agric_data[flagObservationStatus != "M"]

agric_data_tot <-
  agric_data[,
    .(
      measuredItemCPC = "F1882",
      Value = sum(Value),
      flagObservationStatus = "T", # For now?
      #  aggregateObservationFlag(
      #    flagObservationStatus,
      #    flagTable = flagWeightTable_status
      #  ),
      flagMethod = "s"
    ),
    by = c("geographicAreaM49", "measuredElementTrade", "timePointYears")
  ]

setcolorder(agric_data_tot, names(agric_data))

stats_agric <- SaveData("trade", "total_trade_cpc_m49", agric_data_tot, waitTimeout = Inf)


if (!CheckDebug()) {
#  updateInfoTable(
#    year    = year,
#    table   = 'total_tf_runs_info',
#    mode    = 'save',
#    results = stats
#  )

  send_mail(
    from    = "SWS-trade-module@fao.org",
    to      = EMAIL_RECIPIENTS,
    subject = paste0("Total trade plugin (year ",  year, ") ran successfully"),
    body    = end_message
  )
}

print(end_message)

