
dev_sws_set_file <- "modules/complete_tf_cpc/sws.yml"

# Switch off dplyr's progress bars globally
options(dplyr.show_progress = FALSE)

# max.print in RStudio is too small
options(max.print = 99999L)

# Libraries ####
suppressPackageStartupMessages(library(data.table))
library(stringr)
library(magrittr)
library(scales)
library(tidyr, warn.conflicts = FALSE)
library(futile.logger)
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



corrections <- readRDS(gzcon(url('http://campbells-fao:3838/mongeau/files/corrections_table.rds')))
corrections = data.frame(corrections)
dim(corrections)


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
for(i in 1:nrow(corrections)) {

  data = getSubsetDataSWS(reporter = corrections[i, 1],
                          partner = corrections[i, 2],
                          item = corrections[i, 4],
                          elements = c("5610", "5622", "5630",
                                       "5910", "5922", "5930"),
                          year = as.character(corrections[i, 3]))

  data$flow = ifelse(
    substr(data$measuredElementTrade, 1, 2) == "56", 1, 2)

  # dcast.data.table(data, )

  data[, lastElement := substr(data$measuredElementTrade, 3, 4)]

  # check whether the partner has a mirror data or not

  oldQtyReporter = data[geographicAreaM49Reporter == corrections[i, 1] & flow == corrections[i, 5] &
                          lastElement == 10, Value]

  conditional = data[geographicAreaM49Partner == corrections[i, 1] & flow == ifelse(corrections[i, 5] == 1, 2, 1) &
         lastElement == 10]

    isMirror = round(conditional$Value[1], 3) == round(oldQtyReporter, 3) &
      conditional$flagObservationStatus[1] == "E"


  ## Modifying reporter:compute the qty

  data[geographicAreaM49Reporter == corrections[i, 1] &
         flow == corrections[i, 5] &  lastElement == 10 &
         round(Value, 3) == round(corrections[i, 6], 3),
       Value := corrections[i, 10]]

  ## to compute the unit value

  oldMonetValueReporter = data[geographicAreaM49Reporter == corrections[i, 1] &
                                 flow == corrections[i, 5] & lastElement == 22, Value]

  newQtyReporter = data[geographicAreaM49Reporter == corrections[i, 1] &
                          flow == corrections[i, 5] &  lastElement == 10, Value]

  # computing new unit value
  data[geographicAreaM49Reporter == corrections[i, 1] &
         flow == corrections[i, 5] & lastElement == 30, Value := oldMonetValueReporter/newQtyReporter * 1000]

  ## Change flagObservationStatus for qty and unit value

  data[geographicAreaM49Reporter == corrections[i, 1] &
         flow == corrections[i, 5] &  lastElement == 10, flagObservationStatus := "I"]

  data[geographicAreaM49Reporter == corrections[i, 1] &
         flow == corrections[i, 5] & lastElement == 30, flagObservationStatus := "I"]

  # Change flagMethod for qty
  data[geographicAreaM49Reporter == corrections[i, 1] &
         flow == corrections[i, 5] &  lastElement == 10, flagMethod := "e"]


  if(isMirror == T) {

    # modify partner

    data[geographicAreaM49Partner == corrections[i, 1] & flow != corrections[i, 5] &
           lastElement == 10, Value := newQtyReporter]

    oldMonetValuePartner = data[geographicAreaM49Partner == corrections[i, 1] &
                                  flow != corrections[i, 5] & lastElement == 22, Value]

    data[geographicAreaM49Partner == corrections[i, 1] & flow != corrections[i, 5] &
           lastElement == 30, Value := oldMonetValuePartner/newQtyReporter * 1000]

  }

  data[, c("flow", "lastElement") := NULL]

  toBeFilled[[i]] = data

}

toBeFilled = do.call(rbind, toBeFilled)

stats <- SaveData("trade",
                  "completed_tf_cpc_m49",
                  toBeFilled,
                  waitTimeout = 10800)

stats
