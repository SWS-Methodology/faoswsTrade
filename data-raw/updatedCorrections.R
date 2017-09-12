
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
if(faosws::CheckDebug()){
  set_sws_dev_settings(dev_sws_set_file)
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

  data$flow = ifelse(
    substr(data$measuredElementTrade, 1, 2) == "56", 1, 2)

  data[, lastElement := substr(data$measuredElementTrade, 3, 4)]

  data$type = ifelse(
    substr(data$lastElement, 1, 2) %in% c("08", "09", "10"), "qty", ifelse(substr(data$lastElement, 1, 2) == "22", "value", "unit_value"))

  # check whether the partner has a mirror data or not

  oldQtyReporter = data[geographicAreaM49Reporter == corr_reporter & flow == corr_flow &
                          type == "qty", Value]

  conditional = data[geographicAreaM49Partner == corr_reporter & flow == ifelse(corr_flow == 1, 2, 1) &
         type == "qty"]

  if (nrow(conditional) > 0) {
    isMirror = round(conditional$Value[1], 3) == round(oldQtyReporter, 3) &
        conditional$flagObservationStatus[1] == "T"
  }


  ## Modifying reporter: compute the qty

  data[geographicAreaM49Reporter == corr_reporter &
         flow == corr_flow & type == "qty" &
         round(Value, 3) == round(corr_data_orig, 3),
       Value := corr_input]

  ## to compute the unit value

  oldMonetValueReporter = data[geographicAreaM49Reporter == corr_reporter &
                                 flow == corr_flow & type == "value", Value]

  newQtyReporter = corr_input

  # computing new unit value
  data[geographicAreaM49Reporter == corr_reporter &
         flow == corr_flow & type == "unit_value", Value := oldMonetValueReporter/newQtyReporter * 1000]

  ## Change flagObservationStatus for qty and unit value

  data[geographicAreaM49Reporter == corr_reporter &
         flow == corr_flow & type == "qty", flagObservationStatus := "I"]

  data[geographicAreaM49Reporter == corr_reporter &
         flow == corr_flow & type == "unit_value", flagObservationStatus := "I"]

  # Change flagMethod for qty
  data[geographicAreaM49Reporter == corr_reporter &
         flow == corr_flow & type == "qty", flagMethod := "e"]


  if (isMirror == TRUE) {

    # modify partner

    data[geographicAreaM49Partner == corr_reporter & flow != corr_flow &
           lastElement == 10, Value := newQtyReporter]

    oldMonetValuePartner = data[geographicAreaM49Partner == corr_reporter &
                                  flow != corr_flow & lastElement == 22, Value]

    data[geographicAreaM49Partner == corr_reporter & flow != corr_flow &
           lastElement == 30, Value := oldMonetValuePartner/newQtyReporter * 1000]

  }

  data[, c("flow", "lastElement", "type") := NULL]

  toBeFilled[[i]] = data

}

toBeFilled = do.call(rbind, toBeFilled)

# Remove duplicates
toBeFilled <- distinct(toBeFilled)

stats <- SaveData("trade",
                  "completed_tf_cpc_m49",
                  toBeFilled,
                  waitTimeout = 10800)

stats
