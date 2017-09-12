
dev_sws_set_file <- "modules/complete_tf_cpc/sws.yml"

# Libraries ####
suppressPackageStartupMessages(library(data.table))
library(tidyr, warn.conflicts = FALSE)
suppressPackageStartupMessages(library(dplyr, warn.conflicts = FALSE))
library(faosws)
library(faoswsUtil)
library(faoswsTrade)
library(faoswsFlag)

# Development (SWS-outside) mode addons ####
if (faosws::CheckDebug()){
  SETTINGS <- faoswsModules::ReadSettings(dev_sws_set_file)
  ## Define where your certificates are stored
  faosws::SetClientFiles(SETTINGS[["certdir"]])
  ## Get session information from SWS. Token must be obtained from web interface
  faosws::GetTestEnvironment(baseUrl = SETTINGS[["server"]],
                             token   = SETTINGS[["token"]])
} else {
  # In order to have all columns aligned. Issue #119
  options(width = 1000L)

  # Remove domain from username
  USER <- regmatches(
    swsContext.username,
    regexpr("(?<=/).+$", swsContext.username, perl = TRUE)
  )

  options(error = function(){
    dump.frames()
    filename <- file.path(Sys.getenv("R_SWS_SHARE_PATH"),
                          USER,
                          "complete_tf_cpc")
    dir.create(filename, showWarnings = FALSE, recursive = TRUE)
    save(last.dump, file = file.path(filename, "last.dump.RData"))
  })
}

if (CheckDebug()) {
  corrections <- readRDS('//hqlprsws1.hq.un.fao.org/sws_r_share/trade/validation_tool_files/corrections_table.rds')
} else {
  corrections <- readRDS('/work/SWS_R_Share/trade/validation_tool_files/corrections_table.rds')
}

corrections = data.frame(corrections)

getSubsetDataSWS <- function(reporter = NA, partner = NA, item = NA, elements = NA, year = NA) {
  # TODO: use error handling
  key <- DatasetKey(domain  = 'trade',
                    dataset = 'completed_tf_cpc_m49',
                    dimensions = list(
                      Dimension(name = 'geographicAreaM49Reporter', keys = c(reporter, partner)),
                      Dimension(name = 'geographicAreaM49Partner',  keys = c(reporter, partner)),
                      Dimension(name = 'measuredItemCPC',           keys = item),
                      Dimension(name = 'measuredElementTrade',      keys = elements),
                      Dimension(name = 'timePointYears',            keys = year)))

  GetData(key = key, omitna = FALSE)
}


toBeFilled = list()
orig_data = list()
for(i in 1:nrow(corrections)) {

  if (CheckDebug()) {
    print(i)
    flush.console()
  }

  isMirror <- FALSE # default

  corr_reporter  <- corrections[i, 'reporter']
  corr_partner   <- corrections[i, 'partner']
  corr_item      <- corrections[i, 'item']
  corr_year      <- corrections[i, 'year']
  corr_flow      <- corrections[i, 'flow']
  corr_data_orig <- corrections[i, 'data_original']
  corr_input     <- corrections[i, 'correction_input']
  corr_data_type <- corrections[i, 'data_type']

  data = getSubsetDataSWS(reporter = corr_reporter,
                          partner  = corr_partner,
                          item     = corr_item,
                          elements = c("5608", "5609", "5610", "5622", "5638", "5639", "5630",
                                       "5908", "5909", "5910", "5922", "5938", "5939", "5930"),
                          year     = as.character(corr_year))

  # Remove self trade
  data = data[geographicAreaM49Reporter != geographicAreaM49Partner,]

  # Some values can be NA if they were "refreshed"
  data = data[!is.na(Value),]

  orig_data[[i]] <- data

  data$flow = ifelse(substr(data$measuredElementTrade, 1, 2) == "56", 1, 2)

  data$lastElement = substr(data$measuredElementTrade, 3, 4)

  data$type = ifelse(
    data$lastElement %in% c("08", "09", "10"), "qty", ifelse(data$lastElement == "22", "value", "unit_value"))

  # check whether the partner has a mirror data or not

  oldVarReporter = data[geographicAreaM49Reporter == corr_reporter & flow == corr_flow &
                          type == corr_data_type, Value]

  conditionalMirror = data[geographicAreaM49Partner == corr_reporter & flow == ifelse(corr_flow == 1, 2, 1) &
         type == corr_data_type]

  if (nrow(conditionalMirror) > 0) {
    isMirror = conditionalMirror$flagObservationStatus[1] == "T"
    #isMirror = round(conditionalMirror$Value[1], 3) == round(oldVarReporter, 3) &
    #    conditionalMirror$flagObservationStatus[1] == "T"
  }

  # / check whether the partner has a mirror data or not


  oldVarValue <- data[geographicAreaM49Reporter == corr_reporter &
                      flow == corr_flow & type == corr_data_type &
                      round(Value, 3) == round(corr_data_orig, 3), Value]

  # Only if old data is equal to the data that was corrected
  if (round(oldVarValue, 3) == round(corr_data_orig, 3)) {

    ## Modifying reporter: compute the qty or value

    data[geographicAreaM49Reporter == corr_reporter &
           flow == corr_flow & type == corr_data_type,
         Value := corr_input]

    ## to compute the unit value

    Reporter_qty = data[geographicAreaM49Reporter == corr_reporter &
                                   flow == corr_flow & type == 'qty', Value]

    Reporter_value = data[geographicAreaM49Reporter == corr_reporter &
                                   flow == corr_flow & type == 'value', Value]

    # XXX maybe the unit value should be modified outside the loop:
    # there are some (few) cases where there are both qty and value
    # corrections, thus it would be computed twice (with two different
    # values)

    # computing new unit value
    data[geographicAreaM49Reporter == corr_reporter &
         flow == corr_flow & type == "unit_value", Value := Reporter_value / Reporter_qty * 1000]

    ## Change flagObservationStatus for qty and unit value

    data[geographicAreaM49Reporter == corr_reporter &
           flow == corr_flow & type == corr_data_type, flagObservationStatus := "I"]

    data[geographicAreaM49Reporter == corr_reporter &
           flow == corr_flow & type == "unit_value", flagObservationStatus := "I"]

    # Change flagMethod for qty
    data[geographicAreaM49Reporter == corr_reporter &
           flow == corr_flow & type == corr_data_type, flagMethod := "e"]


    if (isMirror == TRUE) {

      # modify partner

      # qty
      data[geographicAreaM49Partner == corr_reporter & flow != corr_flow &
             type == 'qty', Value := Reporter_qty]
      # / qty

      # value
      # (if we corrected an import for the reporter, it was an
      # export for the partner, thus we add 12%, and vice versa)
      Reporter_value_markup <- ifelse(corr_flow == 1, Reporter_value * 1.12, Reporter_value / 1.12)

      data[geographicAreaM49Partner == corr_reporter & flow != corr_flow &
             type == 'value', Value := Reporter_value_markup]
      # /value

      # uv
      # XXX again: this should be done outside the loop
      data[geographicAreaM49Partner == corr_reporter & flow != corr_flow &
             type == 'unit_value', Value := Reporter_value_markup / Reporter_qty * 1000]
      # / uv

    }

    data[, c("flow", "lastElement", "type") := NULL]

    toBeFilled[[i]] = data
  }

}

toBeFilled = do.call(rbind, toBeFilled)

# Remove duplicates
toBeFilled <- distinct(toBeFilled)

stats <- SaveData("trade",
                  "completed_tf_cpc_m49",
                  toBeFilled,
                  waitTimeout = 10800)

stats
