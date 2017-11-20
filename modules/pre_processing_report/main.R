suppressMessages({
  library(data.table)
  library(futile.logger)
  library(faosws)
  library(faoswsUtil)
  library(faoswsTrade)
  library(dplyr, warn.conflicts = FALSE)
})

# Logging level. There are following levels:
# trace, debug, info, warn, error, fatal
# Level `trace` shows everything in the log

# Additional logger for technical data
futile.logger::flog.logger("dev", "TRACE")
futile.logger::flog.threshold("TRACE", name = "dev")

## set up for the test environment and parameters
R_SWS_SHARE_PATH <- Sys.getenv("R_SWS_SHARE_PATH")
DEBUG_MODE <- Sys.getenv("R_DEBUG_MODE")

# This return FALSE if on the Statistical Working System
if (CheckDebug()) {

  message("Not on server, so setting up environment...")

  USER <- if_else(.Platform$OS.type == "unix",
                  Sys.getenv('USER'),
                  Sys.getenv('USERNAME'))

  library(faoswsModules)
  SETTINGS <- ReadSettings("modules/pre_processing_report/sws.yml")

  # If you're not on the system, your settings will overwrite any others
  R_SWS_SHARE_PATH <- SETTINGS[["share"]]

  # Define where your certificates are stored
  SetClientFiles(SETTINGS[["certdir"]])

  # Get session information from SWS. Token must be obtained from web interface
  GetTestEnvironment(baseUrl = SETTINGS[["server"]],
                     token = SETTINGS[["token"]])

  sapply(dir("R", full.names = TRUE), source)

} else {

  # Remove domain from username
  USER <- regmatches(
    swsContext.username,
    regexpr("(?<=/).+$", swsContext.username, perl = TRUE)
  )

  options(error = function(){
    dump.frames()

    filename <- file.path(Sys.getenv("R_SWS_SHARE_PATH"), USER, "PPR")

    dir.create(filename, showWarnings = FALSE, recursive = TRUE)

    save(last.dump, file = file.path(filename, "last.dump.RData"))
  })
}


initial <- file.path(R_SWS_SHARE_PATH, "mongeau")
save    <- file.path(R_SWS_SHARE_PATH, "trade/pre_processing_report")


maxYearToProcess <- as.numeric(swsContext.computationParams$maxYearToProcess)
minYearToProcess <- as.numeric(swsContext.computationParams$minYearToProcess)

if (any(length(minYearToProcess) == 0, length(maxYearToProcess) == 0)) {
  stop("Missing min or max year")
}

if (maxYearToProcess < minYearToProcess) {
  stop('Max year should be greater or equal than Min year')
}

# Select and copy the most updated folders
getSaveMostRecentfolders(initialDir = initial, saveDir = save)

## create a folder with the 6 csv files that will be updated in SWS

ts_all_reports(
  collection_path = save,
  prefix = "complete_tf_cpc", complete = FALSE
)

files <- dir(paste0(save, "/ts_reports"), full.names = TRUE,
             pattern = 'Table_')

if (length(files) != 6) stop('There is at least one missing .csv file.')

allPPRtables <- c("reporters_years", "non_reporting_countries",
                 "numb_records_reporter", "imp_exp_content_check",
                 "qty_values_country_flow_year", "report_missing_data")

# Send technical log messages to a file and console
flog.appender(appender.tee(file.path(save, "development.log")), name = "dev")

flog.info("SWS-session is run by user %s", USER, name = "dev")

flog.debug("User's computation parameters:",
           swsContext.computationParams, capture = TRUE,
           name = "dev")

for (i in 1:length(files)) {

  flog.info("Table: %s: %s", i, files[i], name = "dev")

  tab <- fread(files[i])

  if (i <= 2) {
    names(tab)[3:ncol(tab)] <- paste0("year_", names(tab)[3:ncol(tab)])
  }

  ## Delete
  table <- allPPRtables[i]
  changeset <- Changeset(table)
  newdat <- ReadDatatable(table, readOnly = FALSE)

  AddDeletions(changeset, newdat)
  Finalise(changeset)

  flog.info("Delete table %s: %s", i, allPPRtables[i], name = "dev")
  flog.info("Last update: %s", as.character(as.POSIXct(max(newdat$`__ts`)/1000,
                                    origin = "1970-01-01")), name = "dev")

  ## Add
  AddInsertions(changeset, tab)
  Finalise(changeset)

  flog.info("Upload table %s: %s", i, allPPRtables[i], name = "dev")
}

"Module completed successfully"
